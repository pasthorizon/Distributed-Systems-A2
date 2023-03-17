#create broker
curl -X POST  -H 'Content-Type: application/json' http://127.0.0.1:5000/broker/register
#create broker
curl -X POST  -H 'Content-Type: application/json' http://127.0.0.1:5000/broker/register

#create broker manager
curl -X POST -d '{"ip": "rbm1", "managertype":"0"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/managers/add

# #create broker manager
curl -X POST -d '{"ip": "rbm2", "managertype":"0"}' -H 'Content-Type: application/json' http://127.0.0.1:5000/managers/add