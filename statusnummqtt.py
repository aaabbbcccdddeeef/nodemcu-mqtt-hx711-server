import paho.mqtt.client as mqtt 
import time
import json
from datetime import datetime
import os
import pymongo

broker_address="192.168.77.53"  #MQTT代理地址
status_num_topic = "/statusnum/2"
mqtt_client_name = str(datetime.timestamp(datetime.now()))
database_create_time = str(datetime.now()).split(" ")[0]

myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
mydb = myclient[database_create_time]
mycol = mydb["statusnum"]

def on_connect(client,userdata,flags,rc):
    print("Code:",str(rc))
    # client.subscribe("/sitstatus")
    # print("subscribe message to topic","/sitstatus")

print("creating new instance")
client = mqtt.Client(mqtt_client_name) 
# client.on_message = on_message 
client.on_connect = on_connect

print("connecting to broker")
client.connect(broker_address) 
client.loop_start()

while True:
    try:
        time.sleep(3)
        statusmqtt = {}
        query = {"status":1}
        mydoc = mycol.find(query).sort("timestamp",-1)
        timestamp = []
        for i in mydoc:
            timestamp.append(i["timestamp"])
        statusmqtt["1"] = len(timestamp)
        statusmqtt["timestamp1"] = timestamp

        query = {"status":0}
        mydoc = mycol.find(query).sort("timestamp",-1)
        timestamp = []
        for i in mydoc:
            timestamp.append(i["timestamp"])
        statusmqtt["0"] = len(timestamp)
        statusmqtt["timestamp0"] = timestamp

        datajson = json.dumps(statusmqtt)

        client.publish(status_num_topic,datajson)
        print("Publish Success~!")
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        print("bye")
        exit(1)

