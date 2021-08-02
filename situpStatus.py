import paho.mqtt.client as mqtt 
import time
import json
from datetime import datetime
import os
import pymongo

database_create_time = str(datetime.now()).split(" ")[0]
mqtt_client_name = str(datetime.timestamp(datetime.now()))

myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
dblist = myclient.list_database_names()
if database_create_time in dblist:
    print("The database exists....")

mydb = myclient[database_create_time]
mycol = mydb["statusnum"]

broker_address="192.168.77.53"  #MQTT代理地址
sit_status_topic = "/sitstatus/2"

def on_connect(client,userdata,flags,rc):
    print("Code:",str(rc))
    client.subscribe(sit_status_topic)
    print("subscribe message to topic",sit_status_topic)


def on_message(client, userdata, message):
    # print("message received " ,str(message.payload.decode("utf-8")))
    # print("message topic=",message.topic)
    # print("message qos=",message.qos)
    statusnum = {}
    statusmqtt = {}
    now = datetime.now()
    timestamp = datetime.timestamp(now)
    statusnum["timestamp"] = timestamp
    statusnum["status"] =  int(str(message.payload.decode("utf-8")))
    x = mycol.insert_one(statusnum)
    print(x)
    
    # query = {"status":1}
    # mydoc = mycol.find(query).sort("timestamp",-1)
    # timestamp = []
    # for i in mydoc:
    #     timestamp.append(i["timestamp"])
    # statusmqtt["1"] = len(timestamp)
    # statusmqtt["timestamp1"] = timestamp

    # query = {"status":0}
    # mydoc = mycol.find(query).sort("timestamp",-1)
    # timestamp = []
    # for i in mydoc:
    #     timestamp.append(i["timestamp"])
    # statusmqtt["0"] = len(timestamp)
    # statusmqtt["timestamp0"] = timestamp

    # datajson = json.dumps(statusmqtt)

    # client.publish("/statusnum",datajson)
    # print("Publish Success~!")




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
    
     
