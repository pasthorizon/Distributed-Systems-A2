import requests
from urllib import response 

class myConsumer:
    def __init__(self, topics: list or None, writebroker: str, readbroker: str):
        self.register = {}
        self.readbroker = readbroker
        self.writebroker = writebroker
        if topics is not None:
            self.registerForTopics(topics) 

    def registerForTopics(self, topics:list):
        for topic in topics:
            if topic not in self.register:
                url = self.writebroker + '/consumer/register'
                try:    
                    response = requests.post(url, json = {'topic':topic}, headers = {'Content-Type': 'application/json'})
                    if response.status_code == 200:
                        self.register[topic] = {}
                        self.register[topic]['id'] = response.json()['consumer_id']
                        self.register[topic]['num_partitions'] = response.json()['num_partitions']
                    else:
                        print(f"Error in registering consumer: {topic} => {response.json()['message']}")

                    return dict(response.json())
                
                except Exception as e:
                    print(f"Error: error in connecting with the server => {e}")

        return None

    def login(self, topics: dict):
        for topic in topics.keys():
            if topic not in self.register:
                url = self.writebroker + '/login'
                try:
                    response = requests.get(url, json = {'topic':topic, 'id': topics[topic], 'type': "consumer"}, headers = {'Content-Type': 'application/json'})
                    if response.status_code == 200:
                        self.register[topic] = {}
                        self.register[topic]['id'] = response.json()['consumer_id']
                        self.register[topic]['num_partitions'] = response.json()['num_partitions']
                    else:
                        print(f"Error in logging in with topic {topic} => {response.json()['message']}")
                    
                    return dict(response.json())
                
                except:
                    print("Error: error in connecting with the server")
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

    def getQueueSize(self, topic: str):
        url = self.readbroker + '/size'
        if topic not in self.register:
            print("Error in getting queue size => Topic not subscribed by the consumer")
            return None
        
        try:
            response = requests.get(url,json = {'topic':topic, 'consumer_id': self.register[topic]})
            if response.status_code == 200:
                return response.json()['size']
            else:
                print("Error in getting queue size => " + response.json()['message'])
        except Exception as e:
            print(f"Error: error in connecting with the server => {e}")
        
        return None
    
    def getNextMessage(self, topic: str, partition = None):
        url = self.readbroker + '/consumer/consume'
        if topic not in self.register:
            print("Error in getting next message => Topic not subscribed by the consumer")
            return None
        
        try:
            query = {}
            query['topic'] = topic
            query['consumer_id'] = self.register[topic]['id']
            if partition != None and partition < self.register[topic]['num_partitions']:
                query['partition'] = partition 
            
            response = requests.get(url, json = query)
            if response.status_code == 200:
                return response.json()['message']
            
            else:
                print('Error in getting next message => ' + response.json()['message'])
                return None
        except:
            print("Error: error in connecting with the server")
        
        return None
        
    def getAllTopics(self):
        url = self.writebroker + '/topics'
        try:
            response = requests.get(url)
            return response.json()['topics']
        except:
            print("Error: error in connecting with the server")