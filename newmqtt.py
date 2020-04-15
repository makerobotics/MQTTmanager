from time import gmtime, strftime, sleep
import datetime
import paho.mqtt.client as mqtt
import sqlite3
import json
import os, sys, traceback, subprocess
from collections import  namedtuple

DataRow = namedtuple("DataRow", "topic value timestamp")
buffer = []
lastConsumption = 0
monitoredTopics = {}

# Priority:
# all topiccs in include list which are not in the exclude list
include_topics =  [ "room/OG/",
                    "room/EG/",
                    "room/UG/",
                    "room/consumption",
                    "global/consumption",
                    "garden/"]

include_topics_json = {"zigbee2mqtt/0x00158d0003f0fe94": ["temperature", "humidity", "pressure"]}

include_topics_direct = ["room/UG/presence",
                         "room/OG/presence",
                         "room/EG/presence",
                         "garden/pump"]

include_topics_direct_json = {"zigbee2mqtt/0x00158d00044b4e1e": "contact"}

monitored_topics = ["room/OG/t", "room/EG/t", "room/UG/t", "room/consumption"]

dbFile = "/mnt/nas/mqttdata.db"
#dbFile = "db.db"
NAS_IP = "192.168.2.200"
MQTT_IP = "192.168.2.201"
NAS_PATH = "/mnt/nas"
TIMEOUT = 600 # 10 mn

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("room/#")
    client.subscribe("zigbee2mqtt/#")
    client.subscribe("garden/#")
    client.subscribe("global/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global lastConsumption

    #print("MQTT: " + msg.topic + " --- " + str(msg.payload))
    try:
        updateMonitoredTopics(msg.topic.strip())
    except:
        traceback.print_exc()
    # check for include topics
    for t in include_topics:
        if msg.topic.startswith(t.decode('utf-8')):
            try:
                if not (isInBuffer(msg.topic.strip())):
                    float(msg.payload)
                    buffer.append(DataRow(topic=msg.topic.strip(), value=str(msg.payload.strip()), timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    # Calculate the hourly consumption and add to buffer
                    if "room/consumption" in msg.topic:
                        if lastConsumption > 0:
                            newConsumption = int((float(msg.payload)-lastConsumption)*1000)
                            #buffer.append(DataRow(topic="global/consumption", value=str(newConsumption), timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                            client.publish("global/consumption", newConsumption)
                        lastConsumption = float(msg.payload)
                    return
            except:
                #traceback.print_exc()
                #print("Payload is not a float: "+msg.topic)
                pass
    # check for json include topics
    for k in include_topics_json:
        if(msg.topic == k):
            try:
                loaded_json = json.loads(msg.payload)
                # for each measurement (pressure, temperature, humidity, ...) in the payload
                for x in loaded_json:
                    # for each measurement (pressure, temperature, humidity, ...) in the include list
                    for y in include_topics_json[k]:
                        if x == y:
                            if(not isInBuffer(msg.topic+"_"+x)):
                                buffer.append(DataRow(topic=msg.topic+"_"+x, value=loaded_json[x], timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            except:
                traceback.print_exc()
                print(msg.topic)

def updateMonitoredTopics(topic):
    global monitoredTopics

    if topic in monitored_topics:
        monitoredTopics[topic] = int(datetime.datetime.now().strftime("%s"))
        #print topic + " monitored"

def monitorTopics():
    if monitoredTopics:
        for k,v in monitoredTopics.items():
            elapsed = int(datetime.datetime.now().strftime("%s")) - v
            #print(k + " last received for " + str(elapsed))
            if elapsed > TIMEOUT:
                print("Timeout: " + k + " (" + str(v) + ")")
                client.publish("global/timeout", k + " (" + str(v) + " sec)")

def isInBuffer(newtopic):
    # Passthrough direct topics
    for t in include_topics_direct:
        if newtopic.startswith(t.decode('utf-8')):
            return False
    for t in include_topics_direct_json:
        if newtopic.startswith(t.decode('utf-8')):
            return False
    # check if record from current hour already in buffer
    for top, val, tim in buffer:
        if newtopic == top.decode('utf-8'):
            #print("found topic")
            today = datetime.datetime.now().strftime("%Y-%m-%d %H")
            if(tim.startswith(today)):
                #print("found time")
                return True
    return False

def showBuffer():
    print "Buffer: " + str(len(buffer))
    for top, val, tim in buffer:
        print top, val, tim

def writeBufferToDB():
    #print("MQTT2DB: " + topic + " " + str(payload))
    if(len(buffer) == 0):
        return
    conn = sqlite3.connect(dbFile)
    c = conn.cursor()
    #print "Writing to db..."
    for i in buffer:
        #print "Inserting: "+i.topic
        try:
            c.execute("INSERT INTO table_data(timestamp, topic, value) VALUES (?,?,?)", (i.timestamp, i.topic, float(i.value)))
            conn.commit()
        except:
            traceback.print_exc()
            #print("Could not write: " + i.topic)

    conn.close()
    # clear buffer (use clear() in Python 3)
    buffer[:] = []
    #print "done"

def isMounted(path):
    res = os.path.ismount(path)
    if not res:
        print path + " is not mounted"
    return res

def mountNAS():
    os.system('sudo mount -t nfs 192.168.2.200:/homes/Yann /mnt/nas') #NFS is able to connect remote on sqlite db
    print "Mount path"

def isReachable(host):
    command = ['ping', '-c', '1', host]
    with open(os.devnull, "w") as f:
        res = subprocess.call(command, stdout=f) == 0

    if not res:
        print "host " + host + " is not reachable"
    return res

# MAIN PROGRAM ---------------------------------------------

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_IP, 1883, 60)
client.loop_start()

last_hour = datetime.datetime.now().hour

try:
    while True:
        sleep(20)
        #showBuffer()
        if(datetime.datetime.now().hour != last_hour):
            # connect to the NAS
            if isReachable(NAS_IP):
                if isMounted(NAS_PATH):
                    writeBufferToDB()
                else:
                    mountNAS()
            # Monitor the topics in the monitored topics list
            monitorTopics()
        last_hour = datetime.datetime.now().hour
except KeyboardInterrupt:
    print "Finished"
    client.loop_stop()
