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

app = Flask(__name__)


global sem
sem = threading.Semaphore() # Semaphore for parallel executions

global MAX_TOPICS
MAX_TOPICS = 100000 # Power of 10 only (CAREFUL!!!)

global conn


# TODO Replace raw indexing by CID indexing as given below
# TODO Close exit on termination of main.py file
# TODO cursor.rollback()
# all_brokers table        number of messages, broker id, healthcheck, 
# all_topics: include column for partition metadata 
# {
#    partition_1: {    
#                      broker assignment: 
#                       number of messages:
#                       active: 
#                   },
#   partition_2: {
#                   
#                }
# }                         active partitions list,            

# all producers table       last interaction time
# all consumers: column storing json partition offset     last interaction tmie


def BadRequestResponse(message: str = ""):
    resp = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": 'Bad Request: ' + message
                }),
                status = 400,
                mimetype = 'application/json'
            )
    return resp

def ServerErrorResponse(message: str = ""):
    resp = app.response_class(
                response=json.dumps({
                    "status": "failure", 
                    "message": 'Server Error: ' + message
                }),
                status = 500,
                mimetype = 'application/json'
            )
    return resp

def GoodResponse(response: dict = {}):
    resp = app.response_class(
                response=json.dumps(response), 
                status = 200,
                mimetype = 'application/json'
            )
    
    return resp


def ReturnTopic(topic: str):
    """
        Checks if a topic is present in the database
        Returns (True/False, HTTP Response, Topic Details)
    """
    sem.acquire()
    try: 
        cursor.execute("""SELECT * FROM all_topics WHERE topicname = %s""",(topic,))
        result = cursor.fetchall()
        sem.release()
        cursor.close()
    except:
        sem.release()
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



# TODO Add Locks
# TODO Check tailid initialization
def CreateNewTopic(topic: str, num = 1):
    """ 
        Helper to Create a New Topic, called directly or while creating a producer
        Returns (True/False, HTTP Response)
    """
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM all_brokers WHERE active=1 ORDER BY numberofmessages""")
    all_brokers = cursor.fetchall()

    if len(all_brokers) == 0:
        return False, ServerErrorResponse('no broker available')

    partitions = {}
    for i in range(num):
        dictionary = {
            "broker" : all_brokers[i%len(all_brokers)][0],
            "numberofmessages":0,
            "active": True
        }
        partitions[i] =  dictionary

    cursor.execute("""SELECT COUNT (*) FROM all_topics""")
    id = cursor.fetchone()[0] + 1
    try:
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['topicid', 'topicname', 'producers', 'consumers', 'partitions', 'tailids'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [id, topic, 0,0, json.dumps(partitions), json.dumps([1] * len(partitions))])
        cursor.execute(sql.SQL("""INSERT INTO all_topics ({col_names})
                        VALUES ({col_values})""")
                        .format(col_names=col_names, col_values=col_values)
                    )
        conn.commit()
    except:
        return False, ServerErrorResponse('error in creating topic')
    finally:
        cursor.close()

    return True, GoodResponse({'status':'success', 'message':f'topic {topic} successfully created'})


# Display list of all topics
@app.route("/topics", methods = ["POST"])
def CreateTopic():
    data = request.json
    if "name" in data:
        _, response, result = ReturnTopic(data['name'])             # Check if the topic exists
        if len(result) == 0:
            num = data["partitions"] if "partitions" in data else 1 
            _, response = CreateNewTopic(data['name'], num) 
    else:
        response = BadRequestResponse('topic not sent')
    
    print(response)
    return response   

# B
@app.route("/topics", methods = ['GET'])
def ListTopics():
    cursor = conn.cursor()
    sem.acquire() #
    try:
        cursor.execute("SELECT topicname FROM all_topics")
        all_topics = cursor.fetchall()
        response = GoodResponse({"topics": [t[0] for t in all_topics]})
    except:
        response = ServerErrorResponse('error while getting all topics')
    finally:
        cursor.close()
        sem.release() #
    return response

# C
@app.route("/consumer/register", methods = ["POST"])
def RegisterConsumer():
    data = request.json
    if "topic" in data:
        cursor = conn.cursor()
        sem.acquire()
        try:
            cursor.execute("""SELECT * FROM all_topics WHERE topicname = %s""", (data['topic'],))
            ids = cursor.fetchall()
        except:
            ids = []
        finally:
            cursor.close()
        
        if len(ids) != 0:
            cursor = conn.cursor()
            try:
                consumerID = ids[0][2]
                topicID = ids[0][0]
                returnID = (consumerID * (MAX_TOPICS) + topicID) * 10

                response = GoodResponse({"status": "success", "consumer_id": returnID})
                cursor.execute("""UPDATE all_topics SET consumers = %s WHERE topicname = %s""", (consumerID + 1, data['topic']))
                cursor.execute("""INSERT INTO all_consumers (consumerid, queueoffset) VALUES (%s, %s)""", (str(returnID), 1))
                conn.commit()
            except:
                response = ServerErrorResponse('error in registering the consumer')
            finally:
                cursor.close()
                sem.release()
        else: 
            sem.release()
            response = ServerErrorResponse('topic not present in the database')
            
    else:
        response = BadRequestResponse('topic not sent')
    
    print(response)
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
        Returns producer_id as a good response
    """
    data = request.json
    if "topic" in data:
        topic = data['topic']
        _, response, result = ReturnTopic(topic)
        cursor = conn.cursor()
        try:
            if len(result) == 0:    # Create topic if this topic is not present
                topicID = cursor.fetchone()[0] + 1
                producerID = 1
                flag, response = CreateNewTopic(topic, 1)
                
            else:
                topicID = result[0][0]
                producerID = result[0][3]

            if flag == True:   # Topic created properly
                returnID = (producerID * (MAX_TOPICS) + topicID) * 10 + 1
                # Add another producer to this topic
                cursor.execute("""UPDATE all_topics SET producers = %s WHERE topicname = %s""", (producerID + 1, topic))
                col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['producerid', 'lit'])
                col_values = sql.SQL(',').join(sql.Literal(n) for n in [str(returnID), int(time.time())])
                # Add another producer to the global producer list
                cursor.execute(sql.SQL("""INSERT INTO all_producers ({col_names})
                                VALUES ({col_values})""")
                                .format(col_names=col_names, col_values=col_values)
                            )
                conn.commit()
                returnID = (producerID * (MAX_TOPICS) + topicID) * 10 + 1
                response = GoodResponse({"status": "success", "producer_id": returnID})  
        except:
            response = ServerErrorResponse('error in registering producer')
        finally:
            cursor.close()
            sem.release()
    else:
        response = BadRequestResponse('topic not sent')

    return response


# Producer produces messages
# TODO Implement round robin functionality instead of random producer
# TODO Test Pending
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
        
        # Get tailids of the topic
        tids = json.loads(result[0][4])
        partitions = json.loads(result[0][5])
        
        # Get all active partitions for this topic
        ap = {}
        for x in partitions:
            if partitions[x]['active'] == True:
                ap[x] = partitions[x] 

        # Check if partition index specified is active
        if 'partition' in data and data['partition'] not in ap:
            return ServerErrorResponse('partition not present for this topic') 
            
        # Get partition index
        pindex = data['partition'] if 'partition' in data and data['partition'] in ap else random.randint(0, len(ap) - 1)

        # Change all_producers (the heartbeat, because of this recent interaction)
        sem.acquire()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE all_producers SET list = %s WHERE producerid = %s", (producer_id, int(time.time())))
            conn.commit()
        except:
            pass
        finally:
            cursor.close()
            sem.release()


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

        if br.text['status'] == 'failure':
            return ServerErrorResponse(br.text['message'])
        
        # Generate new form of the metadata
        tids_new = tids
        tids_new[pindex] += 1

        ap[pindex]['numberofmessages'] += 1
        
        # Update metadata (all_topics, all_brokers)
        sem.acquire()
        cursor = conn.cursor()
        try:
            # Update all_topics
            cursor.execute("UPDATE all_topics SET tailids = %s, partitions = %s", (json.dumps(tids_new), json.dumps(ap)))
            # Update all_brokers
            cursor.execute("SELECT numberofmessages FROM all_brokers WHERE brokerid = %s", (bid, ))
            nm = cursor.fetchall()[0][0]
            cursor.execute("UPDATE all_brokers SET numberofmessages = %s WHERE brokerid = %s", (nm + 1, bid))
            conn.commit()
            response = GoodResponse({"status": "success"})
        except:
            response = ServerErrorResponse('error in updating metadata')
        finally:
            cursor.close()
            sem.release()
    else:
        response = BadRequestResponse('topic or producer id not sent')
    
    print(response)
    return response


# F
@app.route('/consumer/consume', methods = ["GET"])
def DequeueMessage():
    data = request.json
    if "topic" in data and "consumer_id" in data:
        resp, result = CheckValidityOfID(id = data['consumer_id'], topic = data['topic'], client = "consumer")
        if result is None: return resp
        cursor = conn.cursor()
        sem.acquire()
        try: 
            cursor.execute("""SELECT * FROM all_consumers WHERE consumerid = %s""",(str(data["consumer_id"]),))
            hid = cursor.fetchone()[1]
        except:
            sem.release()
            cursor.close()
            return ServerErrorResponse('error in accessing all_consumers table')
        
        tid = result[0][4]
        if hid == tid:
            response = ServerErrorResponse('consumer is up to date')
        else:
            try:
                cursor.execute(sql.SQL("""SELECT message 
                                            FROM {table_name} 
                                            WHERE messageid = {hid}""").format(table_name = sql.Identifier(data['topic']), 
                                            hid = sql.Literal(hid)))

                message = cursor.fetchall()[0][0]
                cursor.execute("UPDATE all_consumers SET queueoffset = %s WHERE consumerid = %s", (hid + 1, str(data['consumer_id'])))
                response = GoodResponse({"status": "success", "message": message})
                conn.commit()
            except:
                response = ServerErrorResponse('error in fetching message from the queue')

        sem.release()
        cursor.close()
    
    else:
        response = BadRequestResponse('topic or consumer id not sent')

    print(response)
    return response

# G
@app.route('/size', methods = ['GET'])
def Size():
    print(conn)
    data = request.json
    
    if "topic" in data and "consumer_id" in data:
        resp, result = CheckValidityOfID(data['consumer_id'], data['topic'], "consumer")
        if result is None: return resp
        
        sem.acquire()
        cursor = conn.cursor()
        try: 
            cursor.execute("SELECT tailid FROM all_topics WHERE topicname = %s",(data['topic'],))
            tid = cursor.fetchone()[0]
            cursor.execute(sql.SQL("SELECT queueoffset FROM all_consumers WHERE consumerid={consumerID}").
                        format(consumerID = sql.Literal(str(data['consumer_id']))))
            conn.commit()
            queueoffset = cursor.fetchone()[0]
            response = GoodResponse({"status": "success", "size": tid - queueoffset})
        
        except: 
            response = ServerErrorResponse('consumer is up to date')
        finally:
            cursor.close()
            sem.release()
    else:
        response = BadRequestResponse('topic or consumer id not sent')
    
    print(response)
    return response


@app.route('/login', methods = ['GET'])
def Login():
    data = request.json
    if "topic" in data and "id" in data and "type" in data:
        resp, query = CheckValidityOfID(data['id'], data['topic'], data['type'])
        return GoodResponse({"status": "success"}) if query is not None else resp
    else:
        return BadRequestResponse('topic or consumer id not sent')

@app.route('/broker/heartbeat', methods = ['GET'])
def recordHeartbeat():
    data = request.json
    if "id" in data:
        sem.acquire()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM all_brokers WHERE brokerid = %s",(data["id"],))
        if len(cursor.fetchall()) == 0:
            sem.release()
            return BadRequestResponse('broker id invalid')
        else:
            epochtime = int(time.time())
            cursor.execute("UPDATE all_brokers SET lastheartbeat=%s WHERE brokerid = %s", (epochtime, data["id"]))
            conn.commit()
            sem.release()

            return GoodResponse({"status": "success"})

@app.route('/broker/register', methods = ['POST'])
def createBroker():
    # TODO add lock
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT (*) FROM all_brokers")
    id = cursor.fetchone()[0] + 1
    cursor.execute("""INSERT INTO all_brokers (brokerid, lastheartbeat, numberofmessages, partitions_assigned, active) VALUES 
        (%s, %s, 0, %s, 1)""",(id, int(time.time()),json.dumps({})))


    conn.commit()
    return GoodResponse({'status':'success'})

# Heartbeat from brokers
@app.route("/heartbeat", methods = ['POST'])
def Heartbeat():
    # TODO add lock
    data = request.json
    brokerid = data['broker_id']
    timestamp = time.time()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM all_brokers WHERE brokerid = %s", (brokerid,))
    if len(cursor.fetchone()) == 0:
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
                tailids JSONB,
                partitions JSONB
                )""")

        cursor.execute("""CREATE TABLE all_consumers(
            consumerid VARCHAR(255) PRIMARY KEY,
            partitionoffsets JSONB,
            lit BIGINT)""")
        
        cursor.execute("""CREATE TABLE all_producers(
            producerid VARCHAR(255) PRIMARY KEY,
            lit BIGINT)""")
        
        cursor.execute("""CREATE TABLE all_brokers(
            brokerid SMALLINT PRIMARY KEY,
            lastheartbeat BIGINT,   
            numberofmessages BIGINT,
            partitions_assigned JSONB,
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
    print(all_tables)


    app.run(debug=True, host='0.0.0.0')