import requests
from urllib import response

class myProducer:
    def __init__(self, topics: list or None, broker: str):
        self.register = {}
        self.broker = broker
        if topics is not None:
            self.registerForTopics(topics) 

    def registerForTopics(self, topics:list):
        for topic in topics:
            if topic not in self.register:
                url = self.broker + '/producer/register'
                try:
                    response = requests.post(url, json = {'topic':topic}, headers = {'Content-Type': 'application/json'})
                    
                    if response.status_code == 200:
                        self.register[topic] = {}
                        self.register[topic]["id"] = response.json()['producer_id']
                        self.register[topic]["num_partitions"] = response.json()['num_partitions']

                    else:
                        print(f"Error in creating topic: {topic} => {response.json()['message']}")

                    return dict(response.json())

                except Exception as e:
                    print(f"Error: error in connecting with the server => {e}")
        
        return None

    def login(self, topics: dict):
        for topic in topics.keys():
            if topic not in self.register:
                url = self.broker + '/login'
                try:
                    response = requests.get(url, json = {'topic':topic, 'id': topics[topic], 'type': "producer"}, headers = {'Content-Type': 'application/json'})
                    if response.status_code == 200:
                        self.register[topic] = {}
                        self.register[topic]["id"] = topics[topic]
                        self.register[topic]["num_partitions"] = response.json()['num_partitions']
                    else:
                        print(f"Error in logging in with topic {topic} => {response.json()['message']}")
                    
                    return dict(response.json())
                
                except Exception as e:
                    print(f"Error: error in connecting with the server => {e}")
            
            else:
                print(f'Already logged in with topic {topic}')
        
        return None

    def getID(self, topic: str):
        rv = {}
        if topic in self.register:
            rv[topic] = self.register[topic]["id"] 
        else:
            print(f'Error in getting ID => {topic} not registered by the producer')
            rv[topic] = None
        
        return rv
    
    def getIDs(self, topic: list):
        rv = {}
        for topix in topic:
                if topix in self.register:
                    rv[topix] = self.register[topix]["id"] 
                else:
                    print(f'Error in getting ID => {topix} not registered by the producer')
                    rv[topix] = None
        
        return rv

    def getPartition(self, topic: str):
        rv = {}
        if topic in self.register:
            rv[topic] = self.register[topic]["num_partitions"] 
        else:
            print(f'Error in getting ID => {topic} not registered by the producer')
            rv[topic] = None
        
        return rv

    def getPartitions(self, topic: list):
        rv = {}
        for topix in topic:
                if topix in self.register:
                    rv[topix] = self.register[topix]["num_partitions"] 
                else:
                    print(f'Error in getting ID => {topix} not registered by the producer')
                    rv[topix] = None
        
        return rv

    def sendNewMessage(self, topic: str, message: str, partition = None):
        url = self.broker + '/producer/produce'
        if topic not in self.register:
            print("Error in producing message => Topic not subscribed by the producer")
            return None
        
        query_dict = {}
        query_dict['topic'] = topic 
        query_dict['producer_id'] = self.register[topic]["id"]
        query_dict['message'] = message

        if partition != None and partition < self.register['topic']['num_partitions']:
            query_dict['partition'] = partition

        try:
            response = requests.post(url, json = query_dict)
            if response.status_code == 200:
                pass
            else:
                print('Error in sending message => ' + response.json()['message'])
            return dict(response.json())
            
        except Exception as e:
            print(f"Error: error in connecting with the server => {e}")
        
        return None


    def getAllTopics(self):
        url = self.broker + '/topics'
        response = requests.get(url)
        return response.json()['topics']
