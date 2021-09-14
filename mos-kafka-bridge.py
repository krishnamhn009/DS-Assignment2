# Importing Libraries

import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time

# Connecting MQTT Brokers with local host 

mqttBroker="127.0.0.1"
client=mqtt.Client("P1")
client.connect(mqttBroker)

# Connecting Kafka Prdoucer with local host which works as Bridge between
# MQTT Server and Kafka Server of local host and port number 9092
# Created Topic : topic-vehicle-data

kafka_client=KafkaClient("localhost:9092")
kafka_topic=kafka_client.topics["topic-vehicle-data"]
kafka_producer=kafka_topic.get_sync_producer()

# Reading on screen message from MQTT Server and Kafka Producer

def on_message(client, userdata, message):
    reading_data=str(message.payload.decode("utf-8"))
    split_row=reading_data.split('|')    
    print("MQTT_TRUCK : ", reading_data)
    kafka_producer.produce(str(message.payload).encode('utf-8'))
    print("KAFKA_TRUCK : ", str(message.payload.decode('utf-8')))    

client.loop_start()
client.subscribe("topic-vehicle-data")  
client.on_message = on_message
time.sleep(2) # Reading data for 1 ms for
