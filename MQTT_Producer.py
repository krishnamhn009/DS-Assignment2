# Importing Libraries

import paho.mqtt.client as mqtt
from random import randrange, uniform
import time
import csv

# Connecting MQTT Producer with local host 

mqttBroker="127.0.0.1"
client=mqtt.Client("P0")
client.connect(mqttBroker)

n=0;
while True:
    with open('.\geolocation.csv', mode='r') as csv_file:    
        csv_reader = csv.reader(csv_file)        
        for row in csv_reader:   
            if(row.__contains__("driverid")!=True):         
                row=' | '.join(row)
                client.publish("topic-vehicle-data",row)        
                print(str(row))
                time.sleep(1)
        


        
    