from itertools import product
import threading
from xml.sax.handler import feature_external_ges
from flask import Flask, request
import json
import psycopg2
from psycopg2 import sql
import time
import sys
import random
import requests
from responses import ServerErrorResponse, BadRequestResponse, GoodResponse

app = Flask(__name__)


global semSync
semSync = threading.Semaphore() # Semaphore for parallel executions

global MAX_TOPICS
MAX_TOPICS = 100000 # Power of 10 only (CAREFUL!!!)

global conn


def ReturnTopic(topic: str):
    """
        Checks if a topic is present in the database
        Returns (True/False, HTTP Response, Topic Row Details)
    """
    try: 
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM all_topics WHERE topicname = %s""",(topic,))
        result = cursor.fetchall()
        cursor.close()
    except Exception as e:
        print (e)
        cursor.close()
        return False, ServerErrorResponse('error in accessing server'), []
    
    if len(result) == 0:
        return False, ServerErrorResponse('topic not present in database'), []
    
    return True, GoodResponse({'status':'success'}), result

def CheckValidityOfID(id: int, topic: str, client: str):
    """
        Checks if an ID is valid
        
    """
    ind = 3  if client == "producer" else 2

    flag, response, result = ReturnTopic(topic)
    if flag == False:
        return response, None

    table_id = id // (10 * MAX_TOPICS)
    table_topic = (id // 10) % (MAX_TOPICS)
    flag = id % 2
    correct_flag = 1 if client == "producer" else 0
    if table_topic != result[0][0] or table_id >= result[0][ind] or flag != correct_flag: 
        return BadRequestResponse(f'topic not subscribed by {client}'), None
    else:
        return None, result
    
def Broadcast(sql: str):
    id = int(DB_HOST.replace('rmd', ''))
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_managers")
    try:
        result = cursor.fetchall()
        for man in result:
            
            if man[0] != id and man[3] == 1:
                url = 'http://' + man[2] + ':5000/sync'
                # print(f"\n{DB_HOST} syncing with {man[2]}\n")
                response = requests.post(url, 
                                json = {
                                    "sender": DB_HOST,
                                    "query": sql
                                },
                                headers = {'Content-Type': 'application/json'})

                # print(f"response from {man[2]}: ")
                # print( response.json())       


                if response.status_code != 200:
                    return False
        return True
    
    except Exception as e:
        print(e)
        return False

@app.route("/sync", methods = ['POST'])
def Sync():
    data = request.json
    sql = data["query"]   
    semSync.acquire()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        response = GoodResponse({"message":"sync successful"})
    except Exception as e:
        print(e)
        response =  ServerErrorResponse('unable to process request')
    finally:
        cursor.close()
        semSync.release()
    return response
    

# Get the list of all topics in the write only broker manager
@app.route("/topics", methods = ['GET'])
def ListTopics():
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT topicname FROM all_topics")
        all_topics = cursor.fetchall()
        response = GoodResponse({"topics": [t[0] for t in all_topics]})
    except:
        response = ServerErrorResponse('error while getting all topics')
    finally:
        cursor.close()
    return response

# Consume a message from the queue
@app.route('/consumer/consume', methods = ["GET"])
def DequeueMessage():
    """
        Gets an unread message for consumer with ID = consumer_id
        HTTP Request JSON Format
        {
            "topic": str,
            "consumer_id": str
        }
    """
    data = request.json
    if "topic" in data and "consumer_id" in data:
        topic = data['topic']
        consumer_id = data['consumer_id']
        resp, result = CheckValidityOfID(id = consumer_id, topic = topic, client = "consumer")
        if result is None: return resp
        # Steps
        partitions = result[0][4]
        active_partitions = [x for x in partitions.keys() if partitions[x]["active"] == True]
        if len(active_partitions) == 0:
            # Print no active partitions
            return ServerErrorResponse('no active partition for this topic')
        
        try: 
            cursor = conn.cursor()
            cursor.execute("""SELECT * FROM all_consumers WHERE consumerid = %s""",(str(consumer_id),))
            result2 = cursor.fetchall()
            offsets = result2[0][1]
            lastp = result2[0][2]
            message = ''
            # print(result2)
            # Find the next active partition which has pending messages
            if 'partition' in data:
                cp = data['partition']
                nextp = cp
                bid = partitions[str(cp)]["broker"]
                        # Check if cp gets a response
                response = requests.get("http://" + 'b' + str(bid) + ':5000/dequeue', 
                            json = {
                                "topic": topic, 
                                "partition": cp,
                                "offset": offsets[str(cp)]
                            },
                            headers = {'Content-Type': 'application/json'})
                # print(response.json()['message'])
                if response.json()['status'] == 'success':
                    offsets[str(cp)] += 1
                    message = response.json()['message']

            else:
                for i in range(len(partitions)):
            # print(f"\n {DB_HOST} completed broadcast for {consumer_id}\n")
                    cp = (lastp + i + 1) % len(partitions)
                    nextp = cp
                    # print(partitions[str(cp)])
                    if partitions[str(cp)]["active"] == True:
                        bid = partitions[str(cp)]["broker"]
                        # Check if cp gets a response
                        response = requests.get("http://" + 'b' + str(bid) + ':5000/dequeue', 
                                    json = {
                                        "topic": topic, 
                                        "partition": cp,
                                        "offset": offsets[str(cp)]
                                    },
                                    headers = {'Content-Type': 'application/json'})
                        # print(response.json()['message'])
                        if response.json()['status'] == 'success':
                            offsets[str(cp)] += 1
                            message = response.json()['message']
                            break 

                        else:
                            print(response.json())
            
            if message == '':
                cursor.close()
                return ServerErrorResponse('no message pending in queue')
            
            # print(f"\n\n{DB_HOST} serving {consumer_id} message: {message}")

            
            # Find the next query from this partition
            # Update all_consumers
            query = sql.SQL("""UPDATE all_consumers SET partitionoffsets = {off}, lastindex = {index}
                    WHERE consumerid = {id}""").format(off = sql.Literal(json.dumps(offsets)), 
                                                        index = sql.Literal(nextp), 
                                                        id = sql.Literal(str(consumer_id)))
            cursor.execute(query)
            # ACK synchronize and get ACK for this changes
            # print(f"\n {DB_HOST} calling broadcast for {consumer_id}\n")
            flag = Broadcast(query.as_string(conn))
            # print(f"\n {DB_HOST} completed broadcast for {consumer_id}\n")

            if flag == True:
                conn.commit()
                response = GoodResponse({"status": "success", "message": message + 'from '+ DB_HOST})
            else:
                response = ServerErrorResponse('error in broadcasting')
        except Exception as e:
            print(e)
            response =  ServerErrorResponse('error in ')
        finally:
            cursor.close()
        
    else:
        response = BadRequestResponse('topic or consumer id not sent')

    return response

# G
@app.route('/size', methods = ['GET'])
def Size():
    """
        Gets an unread message for consumer with ID = consumer_id
        HTTP Request JSON Format
        {
            "topic": str,
            "consumer_id": str
        }
        Return
        Partition wise size mentions
        {
            0: int,
            1: int, 
            ...

        }
    """
    data = request.json
    if "topic" in data and "consumer_id" in data:
        topic = data['topic']
        consumer_id = data['consumer_id']
        resp, result = CheckValidityOfID(id = consumer_id, topic = topic, client = "consumer")
        if result is None: return resp
        partitions = result[0][4]
        
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM all_consumers WHERE consumerid = %s", (str(consumer_id),))
            offsets = cursor.fetchall()[0][1]
            cursor.close()
            mess = {}
            for partition in partitions:
                mess[partition] = partitions[partition]["numberofmessages"] - offsets[partition]+1    

            response = GoodResponse({"status": "success", "size": json.dumps(mess)})

        except Exception as e:
            response = ServerErrorResponse(f"Error in accessing offsets => {e}")

        
    else:
        response = BadRequestResponse('topic or consumer id not sent')

    return response


@app.route('/login', methods = ['GET'])
def Login():
    data = request.json
    if "topic" in data and "id" in data and "type" in data:
        resp, query = CheckValidityOfID(data['id'], data['topic'], data['type'])
        return GoodResponse({"status": "success"}) if query is not None else resp
    else:
        return BadRequestResponse('topic or consumer id not sent')

@app.route('/init', methods = ['POST'])
def Init():
    data = request.json
    allTopicsData = data['all_topics']
    allBrokersData = data['all_brokers']
    allManagersData = data['all_managers']
    allProducersData = data['all_producers']
    allConsumersData = data['all_consumers']
    
    cursor = conn.cursor()
    
    for row in allTopicsData:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['topicid', 'topicname', 'consumers', 'producers', 'partitions'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [row[0], row[1], row[2], row[3], json.dumps(row[4])])
    
        query = sql.SQL("""INSERT INTO all_topics ({col_names})
                        VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
        cursor.execute(query)
    
    for row in allProducersData:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['producerid', 'lit'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in row)
        cursor.execute(sql.SQL("""INSERT INTO all_producers ({col_names})
                        VALUES ({col_values})""")
                        .format(col_names=col_names, col_values=col_values))
    
    for row in allBrokersData:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['brokerid', 'lastheartbeat', 'numberofmessages', 'active'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in row)
        cursor.execute(sql.SQL("""INSERT INTO all_brokers ({col_names})
                        VALUES ({col_values})""")
                        .format(col_names=col_names, col_values=col_values))

    for row in allConsumersData:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['consumerid','partitionoffsets', 'lastindex','lit'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [row[0], json.dumps(row[1]), row[2], row[3]])
                # Add another producer to the global consumer list
        query = sql.SQL("""INSERT INTO all_consumers ({col_names})
                                VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
                            
        cursor.execute(query)


    for row in allManagersData:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['managerid','managertype','ip','active'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in row)
                # Add another producer to the global consumer list
        query = sql.SQL("""INSERT INTO all_managers ({col_names})
                                VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
                            
        cursor.execute(query)


    conn.commit()
    return GoodResponse({"status":"success"})

@app.route("/")
def home():
    return f"Hello, World from {DB_HOST}!"
    
if __name__ == "__main__":

    DB_HOST = sys.argv[1]

    DB_NAME = 'dist_queue_3'

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
        cursor.execute("""CREATE TABLE all_topics (
                topicid INT,
                topicname VARCHAR(255) PRIMARY KEY,
                consumers BIGINT,
                producers BIGINT,
                partitions JSONB
                )""")

        cursor.execute("""CREATE TABLE all_consumers(
            consumerid VARCHAR(255) PRIMARY KEY,
            partitionoffsets JSONB,
            lastindex SMALLINT,
            lit BIGINT)""")
        
        cursor.execute("""CREATE TABLE all_producers(
            producerid VARCHAR(255) PRIMARY KEY,
            lit BIGINT)""")
        
        cursor.execute("""CREATE TABLE all_brokers(
            brokerid SMALLINT PRIMARY KEY,
            lastheartbeat BIGINT,   
            numberofmessages BIGINT,
            active SMALLINT
        )
        """)

        cursor.execute("""
            CREATE TABLE all_managers(
                managerid SMALLINT PRIMARY KEY,
                managertype SMALLINT,
                ip VARCHAR(25),
                active SMALLINT
            )
        """)

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


    app.run(debug=True, host='0.0.0.0')