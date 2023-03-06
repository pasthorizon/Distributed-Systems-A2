
from random import randint
from myqueue.consumer import myConsumer
from myqueue.producer import myProducer
import requests

topic_1 = "T-1"
topic_2 = "T-2"
topic_3 = "T-3"


def init(topics):
    for topic in topics:
        numPartitions = randint(1,20)
        resp = requests.post("http://localhost:5000/topics",json = {"name": topic, "partitions":numPartitions },headers = {'Content-Type': 'application/json'})
        # print(resp.json())
        # print(resp.json())

def testInsertMessage():

    topics = ['testInsertMessage-1','testInsertMessage-2', 'testInsertMessage-3']
    producer = myProducer(topics = topics, broker = "http://localhost:5000")


    if list(producer.register.keys()) != topics:
        print("TEST FAILED: Create producer")
        return

    resp = producer.sendNewMessage('testInsertMessage-1', 'testInsertMessage-1-1',0)

    if resp['status'] != 'success':
        print("TEST FAILED: Producer create failed in message insert")
    

    resp = producer.sendNewMessage('testInsertMessage-2', 'testInsertMessage-2-1')
    
    if resp['status'] == 'success':
        print("TEST PASSED: Message insert")
    else:
        print("TEST FAILED: Message insert")

def testRetrieveMessage():
    
    topics = ['testRetrieveMessage-1','testRetrieveMessage-2']

    init(topics)

    producer = myProducer(topics = topics, broker = "http://localhost:5000")
    consumer = myConsumer(topics = topics, writebroker = "http://localhost:5000", readbroker = "http://localhost:8080")
    # print(consumer.register)
    if list(consumer.register.keys()) != topics:
        print("TEST FAILED: Create consumer")
        return

    for i in range(producer.register['testRetrieveMessage-1']['num_partitions']):
        resp = producer.sendNewMessage('testRetrieveMessage-1', 'testRetrieveMessage-1-' + str(i),i)
    flag = True
    for i in range(producer.register['testRetrieveMessage-1']['num_partitions']):
        message = consumer.getNextMessage('testRetrieveMessage-1',i)
        if message != 'testRetrieveMessage-1-' + str(i):
            flag = False
    
    if flag:
        print("TEST PASSED: Message retrieve")
    else:
        print("TEST FAILED: Message retrieve")

def testSize():
    topics = ['testSize-1','testSize-2']

    init(topics)

    producer = myProducer(topics = topics, broker = "http://localhost:5000")
    consumer = myConsumer(topics = topics, writebroker = "http://localhost:5000", readbroker = "http://localhost:8080")

    if list(consumer.register.keys()) != topics:
        print("TEST FAILED: Create consumer")
        return
    
    dictSize = {}

    for i in range(producer.register['testSize-2']['num_partitions']):
        numMessages = randint(1,6)
        dictSize[i] = numMessages
        for j in range(numMessages):
            resp = producer.sendNewMessage('testSize-2', 'testSize-2-' + str(i) + '-' + str(j), i)
    
    if consumer.getQueueSize('testSize-2') == dictSize:
        print("TEST PASSED: Get queue size before consumption")
    else:
        print("TEST FAILED: Get queue size before consumption")

    consumer.getNextMessage('testSize-2', 0)

    dictSize[0]-=1

    if consumer.getQueueSize('testSize-2') == dictSize:
        print("TEST PASSED: Get queue size after consumption")
    else:
        print("TEST FAILED: Get queue size after consumption")


def testGetAllTopics():
    topics = ['testGetAllTopics-1', 'testGetAllTopics-2', 'testGetAllTopics-3']
    init(topics)

    producer = myProducer(topics = ['testGetAllTopics-1'], broker = "http://localhost:5000")

    if set(producer.getAllTopics()) == set(topics):
        print("TEST PASSED: Get all topics")
    else:
        print("TEST FAILED: Get all topics")

if __name__ == "__main__":
    # testGetAllTopics()
    # testInsertMessage()
    # testRetrieveMessage()
    testSize()