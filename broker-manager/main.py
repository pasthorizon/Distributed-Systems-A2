
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

global MAX_TOPICS
MAX_TOPICS = 100000 # Power of 10 only (CAREFUL!!!)

global semCreateTopic, semSync, semProducerRegister, semProduceMessage, semConsumerRegister
semCreateTopic = threading.Semaphore()
semSync = threading.Semaphore()
semProducerRegister = threading.Semaphore()
semProduceMessage = threading.Semaphore()
semConsumerRegister = threading.Semaphore()


global conn
global DB_HOST


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
        print(e)
       
        cursor.close()
        return False, ServerErrorResponse('error in accessing server'), []
    
    if len(result) == 0:
        return False, ServerErrorResponse('topic not present in database'), []

    # print(result)
    
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

def CreateNewTopic(topic: str, num = 1):
    """ 
        Helper to Create a New Topic, called directly or while creating a producer
        Returns (True/False, HTTP Response)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM all_brokers ORDER BY numberofmessages""")
        all_brokers = cursor.fetchall()
        cursor.close()
    except Exception as e:
        print(e)
        

    if len(all_brokers) == 0:
        return False, ServerErrorResponse('no broker available')

    partitions = {}
    j = 0
    for i in range(num):
        while j<=1000*len(all_brokers):
            response = requests.post("http://" + 'b' + str(all_brokers[j%len(all_brokers)][0]) + ':5000/partitions', 
                                    json = {
                                        "topic": topic, 
                                        "partition": i,
                                    },
                                    headers = {'Content-Type': 'application/json'})
            print(response.json()["message"])
            if response.status_code == 200:
                dictionary = {
                    "broker" : all_brokers[j%len(all_brokers)][0],
                    "numberofmessages":0,
                    "active": True
                }
                partitions[i] =  dictionary
                break
            time.sleep(180)
        j+=1
        if(j>1000*len(all_brokers)):
            num = i-1
            break
        
    if num==0:
        return False, ServerErrorResponse('no broker available')

    cursor = conn.cursor()
    try:
        cursor.execute("""SELECT COUNT (*) FROM all_topics""")
        id = cursor.fetchone()[0] + 1
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['topicid', 'topicname', 'producers', 'consumers', 'partitions'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [id, topic, 1, 1, json.dumps(partitions)])
        query = sql.SQL("""INSERT INTO all_topics ({col_names})
                        VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)

        cursor.execute(query)
        flag = Broadcast(query.as_string(conn))
        if flag == True:
            conn.commit()
        if flag == False:
            return False, ServerErrorResponse('error in creating topic')
    except Exception as e:
        print(e)
        cursor.close()
        return False, ServerErrorResponse('error in creating topic')
    finally:
        cursor.close()

    return True, GoodResponse({'status':'success', 'message':f'topic {topic} successfully created'})

def Broadcast(s: str):
    id = 0
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM all_managers")
        result = cursor.fetchall()
        # print(result)
        for man in result:
            url = 'http://'+man[2]+':5000/sync' 
            # print(url, s)
            if man[0] != id and man[3] == 1:
                response = requests.post(url, 
                                json = {
                                    "sender": DB_HOST,
                                    "query": s
                                },
                                headers = {'Content-Type': 'application/json'})

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
        response = GoodResponse({})
    except:
        response =  ServerErrorResponse('unable to process request')
    finally:
        cursor.close()
        semSync.release()
    return response

@app.route("/managers/add",methods = ['POST'])
def AddManagers():
    data = request.json
    
    managertype = data['managertype']
    ip = data['ip']

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT (*) FROM all_managers")
    managerid = cursor.fetchone()[0] + 1
    
    col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['managerid', 'managertype', 'ip', 'active'])
    col_values = sql.SQL(',').join(sql.Literal(n) for n in [managerid,managertype,ip,1])

    query = sql.SQL("""INSERT INTO all_managers 
                ({col_names}) VALUES ({col_values})""").format(col_names = col_names, col_values = col_values)

        # cursor.close()
    data = {}
    cursor.execute("SELECT * FROM all_topics")
    result = cursor.fetchall()
    data['all_topics'] = result

    cursor.execute("SELECT * FROM all_brokers")
    result = cursor.fetchall()
    data['all_brokers'] = result

    cursor.execute("SELECT * FROM all_producers")
    result = cursor.fetchall()
    data['all_producers'] = result

    cursor.execute("SELECT * FROM all_consumers")
    result = cursor.fetchall()
    data['all_consumers'] = result

    cursor.execute("SELECT * FROM all_managers")
    result = cursor.fetchall()
    data['all_managers'] = result


    url = 'http://' + ip +":5000/init"
    requests.post(url, json = data, headers = {'Content-Type': 'application/json'})
        

    print(query.as_string(conn))
    cursor.execute(query)
    flag = Broadcast(query.as_string(conn))
    
    if flag:
        return GoodResponse({"status":"success"})
    else:
        # cursor.close()
        return ServerErrorResponse("Failed to add manager")

# Display list of all topics
@app.route("/topics", methods = ["POST"])
def CreateTopic():

    data = request.json
    if "name" in data:
        _, response, result = ReturnTopic(data['name'])             # Check if the topic exists
        if len(result) == 0:
            num = data["partitions"] if "partitions" in data else 1 
            semCreateTopic.acquire()
            _, response = CreateNewTopic(data['name'], num) 
            semCreateTopic.release()
        else:
            response = ServerErrorResponse('topic already exists')
        # Send partitions to brokers and update the all_brokers metadata
        

    else:
        response = BadRequestResponse('topic not sent')
    
    # print(response)
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

# Producer is registered here
@app.route("/producer/register", methods = ["POST"])
def RegisterProducer():
    """
        Registers a new producer and assigns it to a topic
        HTTP Request JSON
        {
            "topic": str
        }
        Returns (producer_id, number of partitions) as a good response
    """
    data = request.json
    if "topic" in data:
        topic = data['topic']
        semProducerRegister.acquire()
        _, response, result = ReturnTopic(topic)
        cursor = conn.cursor()
        try:
            flag = True
            if len(result) == 0:    # Create topic if this topic is not present
                flag, response = CreateNewTopic(topic, 1)
                _, response, result = ReturnTopic(topic)
                       
            producerID = result[0][3]           
            topicID = result[0][0]
            num_partitions = len(result[0][4].keys())
            if flag == True:   # Topic created properly
                returnID = (producerID * (MAX_TOPICS) + topicID) * 10 + 1
                # Add another producer to this topic
                query = sql.SQL("""UPDATE all_topics SET producers = {con} 
                                    WHERE topicname = {top}""").format(con = sql.Literal(producerID + 1), 
                                                                       top = sql.Literal(topic))
                cursor.execute(query)
                # Broadcast the query
                if Broadcast(query.as_string(conn)) == False:
                    cursor.close()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['producerid', 'lit', 'lastindex'])
                col_values = sql.SQL(',').join(sql.Literal(n) for n in [str(returnID), int(time.time()),0])
                # Add another producer to the global producer list
                cursor.execute(sql.SQL("""INSERT INTO all_producers ({col_names})
                                VALUES ({col_values})""")
                                .format(col_names=col_names, col_values=col_values)
                            )
                
                conn.commit()
                response = GoodResponse({"status": "success", 
                                         "producer_id": returnID,
                                         "num_partitions": num_partitions})  
            else:
                response = ServerErrorResponse('topic not present and error in creating topic')

        except Exception as e:
            print(e)
            response = ServerErrorResponse('error in registering producer')
        finally:
            cursor.close()
            semProducerRegister.release()
    else:
        response = BadRequestResponse('topic not sent')
    return response


# Consumer is registered here
@app.route("/consumer/register", methods = ["POST"])
def RegisterConsumer():
    """
        Registers a consumer and adds it to a topic
        HTTP Request JSON 
        {
            "topic": str
        }
        returns, HTTP Response specifying the ID if successful or failure Response
        HTTP Good Response JSON
        {
            "status": "success",
            "consumer_id": int,
            "num_partitions": int
        }
    """
    data = request.json
    if "topic" in data:
        # update all_topics
        topic = data['topic']
        semConsumerRegister.acquire()
        _, response, result = ReturnTopic(topic)
        # print(result)
        if len(result) == 0: # Topic not present
            response = ServerErrorResponse('topic not present in the database')
        else:
            cursor = conn.cursor()
            topicID = result[0][0]
            consumers = result[0][2]
            num_partitions = len(result[0][4].keys())

            consumerID = (consumers * (MAX_TOPICS) + topicID) * 10
            try:
                query = sql.SQL("""UPDATE all_topics SET consumers = {con} 
                                    WHERE topicname = {top}""").format(con = sql.Literal(consumers + 1), 
                                                                    top = sql.Literal(topic))
                cursor.execute(query)
                if Broadcast(query.as_string(conn)) == False:
                    cursor.close()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                offsets = {}
                partitions = result[0][4]
                for x in partitions:
                    offsets[x] = 1
                
                col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['consumerid', 'lit', 'partitionoffsets', 'lastindex'])
                col_values = sql.SQL(',').join(sql.Literal(n) for n in [str(consumerID), int(time.time()),json.dumps(offsets),0])
                # Add another producer to the global consumer list
                query = sql.SQL("""INSERT INTO all_consumers ({col_names})
                                VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
                            
                cursor.execute(query)
                if Broadcast(query.as_string(conn)) == False:
                    cursor.close()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                conn.commit()
                response = GoodResponse({"status": "success", 
                                         "consumer_id": consumerID,
                                         "num_partitions": num_partitions})

            except:
                response = ServerErrorResponse('error in registering consumer')
            finally:
                semConsumerRegister.release()
                cursor.close()

    else:
        response = BadRequestResponse('topic not sent')
    # print(response)
    return response


# Producer produces messages
@app.route("/producer/produce", methods = ["POST"])
def EnqueueMessage():
    """
        HTTP Request JSON Format
        {
            "topic": str,
            "producer_id": str,
            "message": str, 
            "partition": int (optional)
        }
    """
    data = request.json
    if "topic" in data and "producer_id" in data and "message" in data:
        topic = data['topic']
        producer_id = data['producer_id']
        message = data['message']

        # Check if the ID is valid
        resp, result = CheckValidityOfID(producer_id, topic, "producer")
        if result is None: return resp
        
        # Get partitions of the topic
        partitions = result[0][4]
        
        
        # Get all active partitions for this topic
        ap = {}
        # print(partitions)
        for x in partitions:
            if partitions[x]['active'] == True:
                ap[x] = partitions[x] 
        # print(ap)
        # Check if partition index specified is active
        if 'partition' in data and str(data['partition']) not in ap:
            return ServerErrorResponse('partition not present for this topic') 

        cursor = conn.cursor()
        cursor.execute("SELECT lastindex FROM all_producers WHERE producerid = %s",(str(data["producer_id"]),))
        lastindex = int(cursor.fetchone()[0])
            
        # Get partition index
        if 'partition' in data and str(data['partition']) in ap:
            pindex = str(data['partition']) 
        elif  str((lastindex + 1)%len(partitions)) in ap:
            # print("this")
            pindex = str((lastindex + 1)%len(partitions))
        else:
            # print("or this")
            pindex = list(ap.keys())[random.randint(0, len(ap) - 1)]

        # print(ap)
        # print(pindex)
        # Change all_producers (the heartbeat, because of this recent interaction)
        # No need to update lit for read only managers
        # sem.acquire()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE all_producers SET lit = %s, lastindex = %s WHERE producerid = %s", (int(time.time()), pindex, str(producer_id)))
            conn.commit()
        except:
            pass
        finally:
            cursor.close()
            # sem.release()


        # Send message request to the broker with this id
        bid = ap[pindex]['broker'] 

        # Get broker response
        br = requests.post("http://" + 'b' + str(bid) + ':5000/enqueue', 
                                json = {
                                    "topic": topic, 
                                    "partition": pindex,
                                    "message": message
                                },
                                headers = {'Content-Type': 'application/json'})

        if br.json()['status'] == 'failure':
            return ServerErrorResponse(br.text['message'])
        
        # Generate new form of the metadata
        
        semProduceMessage.acquire()
        cursor = conn.cursor()

        cursor.execute("SELECT partitions FROM all_topics WHERE topicname = %s",(topic,))
        partitions = cursor.fetchone()[0]
        partitions[pindex]['numberofmessages'] += 1
        
        # Update metadata (all_topics, all_brokers)
        # sem.acquire()
        try:
            # Update all_topics (and broadcast)
            query = sql.SQL("UPDATE all_topics SET partitions = {part} WHERE topicname={topic}").format(part = sql.Literal(json.dumps(partitions)), topic = sql.Literal(topic))
            cursor.execute(query)
            flag = Broadcast(query.as_string(conn))
    
            if flag != True:
                cursor.close()
                semProduceMessage.release()
                return ServerErrorResponse("Failed to produce message")

            cursor.execute("SELECT numberofmessages FROM all_brokers WHERE brokerid = %s", (bid, ))
            nm = cursor.fetchall()[0][0]
            cursor.execute("UPDATE all_brokers SET numberofmessages = %s WHERE brokerid = %s", (nm + 1, bid))
            conn.commit()
            response = GoodResponse({"status": "success"})
        except Exception as e:
            print (e)
            response = ServerErrorResponse('error in updating metadata')
        finally:
            cursor.close()
            semProduceMessage.release()
    else:
        response = BadRequestResponse('topic or producer id not sent')
    
    print(response)
    return response


@app.route('/login', methods = ['GET'])
def Login():
    data = request.json
    if "topic" in data and "id" in data and "type" in data:
        resp, query = CheckValidityOfID(data['id'], data['topic'], data['type'])
        return GoodResponse({"status": "success", "num_partitions": len(query[0][4].keys())}) if query is not None else resp
    else:
        return BadRequestResponse('topic or consumer id not sent')

@app.route('/broker/heartbeat', methods = ['GET'])
def recordHeartbeat():
    data = request.json
    if "id" in data:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM all_brokers WHERE brokerid = %s",(data["id"],))
        if len(cursor.fetchall()) == 0:
            return BadRequestResponse('broker id invalid')
        else:
            epochtime = int(time.time())
            cursor.execute("UPDATE all_brokers SET lastheartbeat=%s WHERE brokerid = %s", (epochtime, data["id"]))
            conn.commit()
        

            return GoodResponse({"status": "success"})


@app.route('/broker/register', methods = ['POST'])
def CreateBroker():
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT (*) FROM all_brokers")
    id = cursor.fetchone()[0] + 1
    cursor.execute("""INSERT INTO all_brokers (brokerid, lastheartbeat, numberofmessages, active) VALUES 
        (%s, %s, 0, 1)""",(id, int(time.time())))


    conn.commit()
    return GoodResponse({'status':'success'})

@app.route('/broker/remove', methods = ['POST'])
def RemoveBroker():
    pass


# Heartbeat from brokers
@app.route("/heartbeat", methods = ['POST'])
def Heartbeat():
    data = request.json
    brokerid = data['broker_id']
    timestamp = time.time()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_brokers WHERE brokerid = %s", (brokerid,))
    if len(cursor.fetchall()) == 0:
        return ServerErrorResponse('unidentified heartbeat: broker not present')
    cursor.execute("""UPDATE all_brokers SET lastheartbeat = %s WHERE brokerid = %s""", (timestamp, brokerid))
    conn.commit()
    return GoodResponse({'status': 'success'})


@app.route("/")
def home():
    return "Hello, World!"
    
if __name__ == "__main__":

    DB_HOST = sys.argv[1]

    DB_NAME = 'dist_queue_2'

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
            lastindex SMALLINT,
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

    
    app.run(debug=True, host='0.0.0.0')