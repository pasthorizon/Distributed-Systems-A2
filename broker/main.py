import threading
from flask import Flask, request
import json
import psycopg2
from psycopg2 import sql
import sys
import requests
from urllib import response
import time
from responses import GoodResponse, ServerErrorResponse

app = Flask(__name__)


global sem
sem = threading.Semaphore() # Semaphore for parallel executions

global MAX_TOPICS
MAX_TOPICS = 100000 # Power of 10 only (CAREFUL!!!)

global SLEEP_TIME
SLEEP_TIME = 1

global BROKER_ID
global conn

global BROKER_MANAGER




@app.route("/partitions", methods = ["POST"])
def RegisterNewPartition():
    data = request.json
    topic_name = data['topic']
    partition_id = data['partition']

    cursor = conn.cursor()
    sem.acquire()
    cursor.execute("SELECT * FROM all_partitions WHERE topicname = %s AND partitionid = %s",(topic_name, partition_id))
    result = cursor.fetchall()
    if len(result) != 0:
        sem.release()
        cursor.close()
        response = ServerErrorResponse('partition already present')

    else:
        try:
            response = GoodResponse({
                        "status": "success", 
                        "message": f'Topic {topic_name} created successfully'
                    })
            
            cursor.execute("INSERT INTO all_partitions (topicname, partitionid) VALUES (%s, %s)",
                            (topic_name, partition_id))
            cursor.execute(sql.SQL("""CREATE TABLE {table_name} (
                messageid SERIAL PRIMARY KEY, 
                message TEXT
            )""").format(table_name = sql.Identifier(data['topic'] + '_' + str(partition_id))))
            conn.commit()
        except Exception as e:
            print("from 98",e)
            response = ServerErrorResponse("failed to add partition to database")
        finally:
            cursor.close()
            sem.release()
    
    
    print(response)
    return response   

# Heartbeat done by another thread
def Heartbeat():
    while True:
        url = 'http://' + BROKER_MANAGER + ':5000/heartbeat'
        response = requests.post(url, 
                json = {'broker_id': BROKER_ID}, 
                headers = {'Content-Type': 'application/json'})
        time.sleep(SLEEP_TIME)



@app.route("/enqueue", methods = ["POST"])
def EnqueueMessage():
    data = request.json
    topic = data["topic"]
    partition = data["partition"]
    message = data["message"]

    col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['message'])
    cursor = conn.cursor()
    try:
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [message])
        # Add message to the partition table
        cursor.execute(sql.SQL("""INSERT INTO {table_name} ({col_names}) 
                                  VALUES ({col_values})""").format(table_name = sql.Identifier(topic + "_" + partition), 
                                        col_names = col_names, 
                                        col_values = col_values))
        
        # Update all_partitions metadata
        conn.commit()
        response = GoodResponse({"status": "success"}) 
    except:
        response = ServerErrorResponse('error in adding message to the queue')
    finally:
        cursor.close()
    
    return response

@app.route("/dequeue", methods = ['GET'])
def DequeueMessage():
    data = request.json
    topic = data['topic']
    partition = data['partition']
    offset = data['offset']

    cursor = conn.cursor()
    try:
        cursor.execute(sql.SQL("SELECT COUNT (*) FROM {table_name}").format(
                table_name = sql.Identifier(topic + "_" + str(partition))))
        tid = cursor.fetchone()[0]
        if offset > tid:
            response = ServerErrorResponse('requested partition has no new messages')
        else:
            cursor.execute(sql.SQL("""SELECT message FROM {table_name} WHERE messageid = {hid}""").format(
                                        table_name = sql.Identifier(topic + "_" + str(partition)), 
                                        hid = sql.Literal(offset)))
            message = cursor.fetchall()[0][0]
            response = GoodResponse({"status": "success", "message": str(message)})
    except Exception as e:
        print(e)
        response = ServerErrorResponse('error in removing message from queue')
    finally:
        cursor.close()

    print(response)
    return response



@app.route("/")
def home():
    return "Hello, World from Broker!"
    
if __name__ == "__main__":

    DB_HOST = str(sys.argv[1])
    BROKER_MANAGER = sys.argv[2]
    BROKER_ID = sys.argv[3]
    DB_NAME = 'dist_queue'
    conn = psycopg2.connect(
            host=DB_HOST,
            user="postgres",
            password="admin",
        )

    conn.autocommit = True # Only to create the database, new connection will be created
   
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",(DB_NAME,))
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(sql.SQL("CREATE DATABASE {db_name}")
                .format(db_name = sql.Identifier(DB_NAME)))
        cursor.close()
        conn.close()

        conn = psycopg2.connect(
            host=DB_HOST,
            user="postgres",
            password="admin",
            dbname = DB_NAME
        )

        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE all_partitions (
                topicname VARCHAR(255),
                partitionid INT,
                PRIMARY KEY (topicname, partitionid)
                )""")

        conn.commit()

    cursor.close()
    conn.close()


    conn = psycopg2.connect(
            host=DB_HOST,
            user="postgres",
            password="admin",
            dbname = DB_NAME
        )

    cursor = conn.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

    all_tables = cursor.fetchall()
    # print(all_tables)

    # Heartbeats are sent by a separate thread
    heartbeat_thread = threading.Thread(target = Heartbeat)
    # heartbeat_thread.start()

    app.run(debug=True, host = '0.0.0.0')