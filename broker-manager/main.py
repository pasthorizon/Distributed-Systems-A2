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

# TODO Synchronization
# TODO WAL
# TODO Partition assignment to brokers at the beginning of partition creation
#  

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
        Returns (True/False, HTTP Response, Topic Row Details)
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

def CreateNewTopic(topic: str, num = 1):
    """ 
        Helper to Create a New Topic, called directly or while creating a producer
        Returns (True/False, HTTP Response)
    """
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM all_brokers WHERE active = 1 ORDER BY numberofmessages""")
    all_brokers = cursor.fetchall()
    cursor.close()

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

    sem.acquire()
    cursor = conn.cursor()
    try:
        cursor.execute("""SELECT COUNT (*) FROM all_topics""")
        id = cursor.fetchone()[0] + 1
        col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['topicid', 'topicname', 'producers', 'consumers', 'partitions'])
        col_values = sql.SQL(',').join(sql.Literal(n) for n in [id, topic, 0, 0, json.dumps(partitions)])
        query = sql.SQL("""INSERT INTO all_topics ({col_names})
                        VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
                    
        cursor.execute(query)
        flag = Broadcast(query.as_string)
        if flag == True:
            conn.commit()
        else:
            cursor.close()
            sem.release()
            return False, ServerErrorResponse('error in creating topic: broadcast error')
    except:
        cursor.close()
        sem.release()
        return False, ServerErrorResponse('error in creating topic')
    finally:
        cursor.close()
        sem.release()

    return True, GoodResponse({'status':'success', 'message':f'topic {topic} successfully created'})

def Broadcast(sql: str):
    id = 0
    cursor = conn.cursor()
    sem.acquire()
    try:
        cursor.execute("SELECT * FROM all_managers")
        result = cursor.fetchall()
        for man in result:
            if man[0] != id and man[3] == 1:
                response = requests.post(man[2] + '/sync', 
                                json = {
                                    "sender": DB_HOST,
                                    "query": sql
                                },
                                headers = {'Content-Type': 'application/json'})

                if response.status_code != 200:
                    return False
        return True
    
    except:
        return False

@app.route("/sync", methods = ['POST'])
def Sync():
    data = request.json
    sql = data["sql"]   
    sem.acquire()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        response = GoodResponse({})
    except:
        response =  ServerErrorResponse('unable to process request')
    finally:
        cursor.close()
        sem.release()
    return response

# Display list of all topics
# TODO Add partitions to the brokers
@app.route("/topics", methods = ["POST"])
def CreateTopic():
    data = request.json
    if "name" in data:
        _, response, result = ReturnTopic(data['name'])             # Check if the topic exists
        if len(result) == 0:
            num = data["partitions"] if "partitions" in data else 1 
            _, response = CreateNewTopic(data['name'], num) 
            _, response, result = ReturnTopic(data['name'])
        
        # Send partitions to brokers and update the all_brokers metadata
        partition = json.loads(result[0][4])
        for x in partition:
            broker = x["broker"]
            response = requests.post("http://" + 'b' + str(broker) + ':5000/partition', 
                                json = {
                                    "topic": data['name'], 
                                    "partition": x,
                                },
                                headers = {'Content-Type': 'application/json'})

            if response.status_code != 200:
                # This partition could not be registered and hence is inactive
                partition[x]["active"] = False
                cursor = conn.cursor()
                query = sql.SQL("""UPDATE all_topics SET partitions = {part} 
                                WHERE topicname = {tname}""").format(part = sql.Literal(json.dumps(partition), 
                                                                    tname = data['name']))
                cursor.execute(query)
                if Broadcast(query.as_string) == False:
                    cursor.close()
                    return ServerErrorResponse('error in creating topic: broadcast failed')    
                conn.commit()
                cursor.close()

    else:
        response = BadRequestResponse('topic not sent')
    
    print(response)
    return response   

# Get the list of all topics in the write only broker manager
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

# Consumer is registered here
# TODO update partitionoffsets and lastpartition when created
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
            "consumer_id": int
        }
    """
    data = request.json
    if "topic" in data:
        # update all_topics
        topic = data['topic']
        _, response, result = ReturnTopic(topic)
        if len(result) == 0: # Topic not present
            response = ServerErrorResponse('topic not present in the database')

        else:
            topicID = response[0][0]
            consumers = response[0][2]

            consumerID = (consumers * (MAX_TOPICS) + topicID) * 10
            sem.acquire()
            cursor = conn.cursor()
            try:
                query = sql.SQL("""UPDATE all_topics SET consumers = {con} 
                                    WHERE topicname = {top}""").format(con = sql.Literal(consumers + 1), 
                                                                       top = sql.Literal(topic))
                cursor.execute(query)

                if Broadcast(query.as_string) == False:
                    cursor.close()
                    sem.release()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['consumerid', 'lit'])
                col_values = sql.SQL(',').join(sql.Literal(n) for n in [str(consumerID), int(time.time())])
                # Add another producer to the global consumer list
                query = sql.SQL("""INSERT INTO all_consumers ({col_names})
                                VALUES ({col_values})""").format(col_names=col_names, col_values=col_values)
                            
                cursor.execute(query)
                if Broadcast(query.as_string) == False:
                    cursor.close()
                    sem.release()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                conn.commit()
                response = GoodResponse({"status": "success", "consumer_id": consumerID})

            except:
                response = ServerErrorResponse('error in registering consumer')
            finally:
                cursor.close()
                sem.release()

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
                query = sql.SQL("""UPDATE all_topics SET producers = {con} 
                                    WHERE topicname = {top}""").format(con = sql.Literal(producerID + 1), 
                                                                       top = sql.Literal(topic))
                cursor.execute(query)
                # Broadcast the query
                if Broadcast(query.as_string) == False:
                    cursor.close()
                    sem.release()
                    return ServerErrorResponse('error in registering consumer: broadcast failed')
                
                col_names = sql.SQL(',').join(sql.Identifier(n) for n in ['producerid', 'lit'])
                col_values = sql.SQL(',').join(sql.Literal(n) for n in [str(returnID), int(time.time())])
                # Add another producer to the global producer list
                cursor.execute(sql.SQL("""INSERT INTO all_producers ({col_names})
                                VALUES ({col_values})""")
                                .format(col_names=col_names, col_values=col_values)
                            )
                
                conn.commit()
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
        
        # Get partitions of the topic
        partitions = json.loads(result[0][4])
        
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
        # No need to update lit for read only managers
        sem.acquire()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE all_producers SET lit = %s WHERE producerid = %s", (producer_id, int(time.time())))
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
        ap[pindex]['numberofmessages'] += 1
        
        # Update metadata (all_topics, all_brokers)
        sem.acquire()
        cursor = conn.cursor()
        try:
            # Update all_topics (and broadcast)
            query = sql.SQL("UPDATE all_topics SET partitions = {part}").format(part = json.dumps(ap))
            cursor.execute(query)
            flag = Broadcast(query.as_string)
            if flag == False:
                cursor.close()
                sem.release()
                return ServerErrorResponse('error in updating metadata: broadcast failed')
            
            # Update all_brokers (no need to broadcast this change)
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
            partitions_assigned JSONB,
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
    print(all_tables)


    app.run(debug=True, host='0.0.0.0')