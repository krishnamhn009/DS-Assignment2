# Importing Libraries

from typing import final
from confluent_kafka import Consumer
from time import sleep
from pymongo import MongoClient


# Created KafkaConsoleConsumer, which reads data of Kafka Console from Source and passing
# Mongo DB of Atlas Cloud for Dashboard data presentation for users.

class KafkaConsoleConsumer:
    broker = "localhost:9092"
    topic = "topic-vehicle-data"
    group_id = "consumer-1"
    global client
    global collection

    # Function to listen and catch data of console with given Topic of TRUCK_GEO_LOCATION

    def start_listener(self):
        consumer_config = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'auto.offset.reset': 'largest',
            'enable.auto.commit': 'false',
            'max.poll.interval.ms': '86400000'
        }

        # Connecting to MongoDB of Atlas with given URL and TOPIC TRUCK_GEO_LOCATION
        db_client = MongoClient(
            'mongodb+srv://devanand:ganesha@cluster0.urchn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        truck_location_DB = db_client["GEO_LOCATION"]
        truck_location_DB.drop_collection("topic-vehicle-data")
        data_collection = truck_location_DB["topic-vehicle-data"]

        consumer = Consumer(consumer_config)
        consumer.subscribe([self.topic])

        try:
            while True:
                message = consumer.poll(0)
                if message is None:
                    print("Listening...")
                    sleep(5)
                    continue
                if message.error():
                    print("Error reading message : {}".format(message.error()))
                    continue
                # You can parse message and save to data base here
                message = message.value()
                message = message.decode("utf-8")
                print("Reading Consumer Console : ---> ", message)
                try:
                    message = message.strip()
                    split_message = message.split('|')
                    if (split_message[0] != ""):
                        temp_0 = split_message[0].split('b\'')
                        truckid = temp_0[1].strip()
                        driverid = split_message[1].strip()
                        event = split_message[2].strip()
                        latitude = split_message[3].strip()
                        latitude = float(latitude)
                        longitude = split_message[4].strip()
                        longitude = float(longitude)
                        city = split_message[5].strip()
                        state = split_message[6].strip()
                        velocity = split_message[7].strip()
                        velocity = int(velocity)
                        event_ind = split_message[8].strip()
                        event_ind = int(event_ind)
                        temp_1 = split_message[9].split('\'')
                        idling_ind = temp_1[0].strip()
                        idling_ind = int(idling_ind)

                        # Mongo DB scema storing data of truck with various fields as coordinates and different events

                        truck_location_schema = {"TRUCK_ID": truckid,
                                                 "DRIVER_ID": driverid,
                                                 "EVENT": event,
                                                 "LOCATION": {"type": "Point", "coordinates": [longitude, latitude]},
                                                 "CITY": city,
                                                 "STATE": state,
                                                 "VELOCITY": velocity,
                                                 "EVENT_INDICATOR": event_ind,
                                                 "IDLING_INDICATOR": idling_ind
                                                 }
                        data_collection.insert_one(truck_location_schema)
                        print("\nInserting in MongoDB : ---> ", truck_location_schema)
                        consumer.commit()
                except Exception as ex:
                    continue

        except Exception as ex:
            print("Kafka Exception : {}", ex)

        finally:
            print("Closing Consumer After Final Catch")
            consumer.close()
            db_client.close()


my_consumer = KafkaConsoleConsumer()
my_consumer.start_listener()
