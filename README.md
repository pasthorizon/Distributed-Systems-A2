# Distributed Systems (CS60002) : Assignment 2

This repository contains the implementation of [Assignment 2](https://cse.iitkgp.ac.in/~sandipc/courses/cs60002/DS_asgn-2.pdf) of the [Distributed Systems (CS60002)](http://cse.iitkgp.ac.in/~sandipc/courses/cs60002/cs60002.html) course at IIT Kharagpur offered in Spring 2023 by [Prof Sandip Chakraborty](http://cse.iitkgp.ac.in/~sandipc/)

### Contributors
[Rajas Bhatt](https://github.com/dope-dependent) (19CS30037)   
[Seemant G. Achari](https://github.com/pasthorizon) (19CS30057)

### Deployment
This system has been deployed on Docker. There can be other ways of deploying it as well, for example, a virtual machine for every host. 

# Running Instructions
1. In the main directory, run `sudo docker compose up`
2. Run `init.sh` to add brokers and read only broker managers, currently the number of brokers and read broker managers is set to 2.

# Design and Documentation
Design: [https://pasthorizon.github.io/Distributed-Systems-A2/Design.html](https://pasthorizon.github.io/Distributed-Systems-A2/Design.html)


Documentation: [https://pasthorizon.github.io/Distributed-Systems-A2/Documentation.html](https://pasthorizon.github.io/Distributed-Systems-A2/Documentation.html)

# Testing Instructions
1. To test client libraries `consumer.py` and `producer.py`, run `test.py`

2. To perform load testing run `test.sh` and then run `testLoad.py`

3. To reset the system, run `reset.sh`



