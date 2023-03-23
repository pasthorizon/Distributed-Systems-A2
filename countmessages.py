file_name = "./test/P" + str(3) + ".txt"
log_file = open(file_name, "r")

topics = {'T-1':{}, 'T-2':{},'T-3':{}}


for log in log_file:
    topic = log.split()[2]
    partition = log.split()[3]
    if partition in topics[topic]:
        topics[topic][partition]+=1
    else:
        topics[topic][partition]=1
    if topic == 'T-1' and partition == str(0):
        print(log)

print(topics)