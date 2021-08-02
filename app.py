import paho.mqtt.client as mqtt 
import time
import json
import numpy as np
from datetime import datetime
import os
import pymongo

database_create_time = str(datetime.now()).split(" ")[0]

myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
dblist = myclient.list_database_names()
if database_create_time in dblist:
    print("The database exists....")
    mydb = myclient[database_create_time]
    mycol = mydb["status"]

mydb = myclient[database_create_time]
mycol = mydb["status"]

broker_address="192.168.77.53"  #MQTT代理地址
cvvalue = 400  #变异系数
datanum = 20 #统计数组元素个数
stablenum = 3 #数据稳定次数

data = []
status = {}
statusNum = {}
count = 0
top = 0
sitStatus = 0
reg = 0
hardware_topic = "/weight/2"
front_end_topic = "/status/2"
sit_status_topic = "/sitstatus/2"
mqtt_client_name = str(datetime.timestamp(datetime.now()))

def on_connect(client,userdata,flags,rc):
    print("Code:",str(rc))
    client.subscribe(hardware_topic)
    print("subscribe message to topic",hardware_topic)


def on_message(client, userdata, message):
    # print("message received " ,str(message.payload.decode("utf-8")))
    # print("message topic=",message.topic)
    # print("message qos=",message.qos)
    global count
    global top
    global datanum
    global cvvalue
    global stablenum
    global sitStatus
    global reg
    global data
    global status
    global statusNum
    global front_end_topic
    global sit_status_topic


    signal = json.loads(str(message.payload.decode("utf-8")))
    data.append(signal["weight"])
    if len(data) == datanum:
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        datanp = np.asarray(data)
        # datanp = datanp.reshape(2,10)
        # print("0:",np.std(datanp[0]))
        # print("1:",np.std(datanp[1]))
        # print("difference:",np.std(datanp[1])-np.std(datanp[0]))
        std = np.std(datanp)
        avg = np.average(datanp)
        cv = (std / avg)*1000000
        cv = round(cv,5)

        print("***************************************")
        print("STD:",std)
        print("AVG:",avg)
        print("CV:",cv)
        if cv <= cvvalue:
            count += 1
            print("count:",count)
            status["time"] = now
            status["timestamp"] = timestamp
            status["status"] = 0
            status["cv"] = cv
            status["signal"] = data
            # print(status)
            if count == stablenum:
                x = mycol.insert_one(status)
                sitStatus = 0
                count = 0 
                print(x)
                client.publish(front_end_topic,"0")
            status.clear()
        else:
            count = 0



        if cv > cvvalue:
            top += 1
            print("top:",top)
            status["time"] = now
            status["timestamp"] = timestamp
            status["status"] = 1
            status["cv"] = cv
            status["signal"] = data
            # print(status)
            if top == stablenum:
                x = mycol.insert_one(status)
                sitStatus = 1
                top = 0 
                print(x)
                client.publish(front_end_topic,"1")
            status.clear()
        else:
            top = 0
        
        if reg != sitStatus:
            # statusNum["timestamp"] = timestamp
            # statusNum["status"] = sitStatus
            # x = mycolstatusnum.insert_one("1")
            # print(x)
            client.publish(sit_status_topic,str(sitStatus)) 
            print(sit_status_topic + "Publish!")

        reg = sitStatus

        print("timestamp:",timestamp)
        print("***************************************")
        data.clear()
        

print("creating new instance")
client = mqtt.Client(mqtt_client_name) 
client.on_message = on_message 
client.on_connect = on_connect

print("connecting to broker")
client.connect(broker_address) 

while True:
    try:
        client.loop_start()
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        print("bye")
        exit(1)
    
     
