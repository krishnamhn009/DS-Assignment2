from Models import VehcleDriver
import json
from time import sleep
from kafka import KafkaConsumer
from json2object import jsontoobject as jo
from pymongo import MongoClient
dbClient = MongoClient("mongodb+srv://test:test@cluster0.fmqjd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")





def commit_data(data):
        # Connecting to MongoDB of Atlas with given URL and TOPIC TRUCK_GEO_LOCATION
        db = dbClient.test
        rec_id1 = db.insert_one(data)
        dbClient.close()



if __name__ == '__main__':
    parsed_topic_name = 'topic-vehicle-data'
    # Notify if a driver's bp has more than 120/90 psi
    # Notify if a driver's speed is more than 80 km/h
    speed_threshold = 80
    bp_threshold = 120/90
    fuel_level=20

    while True:
        consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                                bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        for msg in consumer:
            try:
                record = json.loads(msg.value)
                driver = VehcleDriver()
                result = jo.deserialize(record, driver)
                if result.vechileFuelLevel <= fuel_level:
                    print(f'Alert: fuel level is below for vehcile  {result.vechileLiecense} located at {result.latitude}- {result.longitude}')
                    commit_data(result)
                if result.vechileSpeed >= speed_threshold:
                    print(f'Alert: {result.driverName} is driving with higher speed.Truck is {result.vechileLiecense} located at {result.latitude}- {result.longitude}')
                    commit_data(result)
            except Exception as ex:
                sleep(3)
            sleep(3)

        if consumer is not None:
            consumer.close()
