#create broker
# curl -X POST  -H 'Content-Type: application/json' http://127.0.0.1:5000/broker/register

# create topic
# curl -X POST -d '{"name": "producer_login", "partitions":' -H 'Content-Type: application/json' http://127.0.0.1:5000/topics

#create broker manager
# curl -X POST -d '{"ip": "rbm1", "managertype":"0"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/managers/add


#create producer and consumer
# curl -X POST -d '{"topic": "producer_login"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register
# curl -X POST -d '{"topic": "producer_signup"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/register

# #produce two messages
# curl -X POST -d '{"topic": "producer_login","producer_id":21, "message": "hello into partition 1 message 0", "partition":1}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce
# curl -X POST -d '{"topic": "producer_signup","producer_id":10000000000001, "message": "hello2"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/producer/produce


# #check size to be 2
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size

# first message to be retrieved
curl -X GET  -d '{"topic": "producer_login","consumer_id":1000020, "partition":1}' -H 'Content-Type: application/json' http://127.0.0.1:5002/consumer/consume

# #queue size should change to 1
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size

# Retrieve Second Message
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/consumer/consume

# Check size should be zero
# curl -X GET  -d '{"topic": "producer_signup","consumer_id":10000000000000}' -H 'Content-Type: application/json' http://127.0.0.1:5000/size

# Check Broker Partition Assignment Register
# curl -X POST -d '{"name": "producer_signup", "partition_id": 1}' -H 'Content-Type: application/json' http://127.0.0.1:5001/partitions

# curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:5000/broker/register


# curl -X POST  -d '{"broker_id":1}' -H 'Content-Type: application/json' http://127.0.0.1:5000/heartbeat
