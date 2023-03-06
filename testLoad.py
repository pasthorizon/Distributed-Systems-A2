import threading
import time
from random import randint

from myqueue.consumer import myConsumer
from myqueue.producer import myProducer
topic_1 = "T-1"
topic_2 = "T-2"
topic_3 = "T-3"

def consumer_func(consumer_id, topics : list):
    file_name = "./test/C_" + str(consumer_id) + ".txt"
    log_file = open(file_name,"w")
    consumer = myConsumer(topics = topics, writebroker = "http://localhost:5000", readbroker = "http://localhost:8080")
    count = 0
    while count<20:

        for topic in topics:
            time.sleep(1)
            log = consumer.getNextMessage(topic)
            if log is not None:
                log_file.write(log + '\n')
                count=0
            else:
                count+=1


def producer_func(producer_id, topics: list):

    producer = myProducer(topics = topics, broker = "http://localhost:5000")
    file_name = "./test/P" + str(producer_id) + ".txt"
    log_file = open(file_name, "r")

    for log in log_file:

        sleep_time = randint(20,60) / 60
        time.sleep(sleep_time)
        topic = log.split()[2]
        partition = log.split()[3]
        producer.sendNewMessage(topic, log.split()[4], partition) 



t1 = threading.Thread(target=producer_func, args=(1,['T-1','T-2']))
t2 = threading.Thread(target=producer_func, args=(2,['T-2', 'T-3']))
t3 = threading.Thread(target=producer_func, args=(3,['T-3', 'T-1']))



t6 = threading.Thread(target=consumer_func, args=(1,['T-2', 'T-3']))
t7 = threading.Thread(target=consumer_func, args=(2,['T-1']))
t8 = threading.Thread(target=consumer_func, args=(3,['T-1', 'T-2', 'T-3']))



# t1.start()
# t2.start()
# t3.start()

t6.start()
t7.start()
t8.start()


# t1.join()
# t2.join()
# t3.join()

# t6.join()
# t7.join()
# t8.join()
