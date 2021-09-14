from Models import VehcleDriver
from time import sleep
from random import randint
import random
from faker import Faker
import json
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def simulateVehicleData(x): 
  
    # dictionary 
    driver_data =[] 
    vehcle_list = ["Tata Pickup", "Tata Turbo xls",
             "Eicher Motor Turbo", "Gati Transport"]
    state_list = ["UP", "BR",
             "DL", "KA","UK"]
   
    fake = Faker(['en_IN'])
    for i in range(0, x): 
        driver= VehcleDriver() 
        driver.id= randint(1, 100) 
        driver.driverName= fake.name() 
        driver.driverTempreture= randint(20,60)
        driver.driverBloodPressure= str(randint(0,200))+'/'+ str(randint(60,90))+'mmHg'
        driver.address= fake.city() 
        driver.latitude= str(fake.latitude()) 
        driver.longitude= str(fake.longitude()) 
        driver.vechileModel= str(random.choice(vehcle_list))
        driver.vechileLiecense= str(random.choice(state_list))+'-'+str(randint(10, 99))+'-'+str(randint(1000, 9999)) # UP-60-9999
        driver.vechileTempreture= randint(30, 150)
        driver.vechileFuelLevel= randint(1, 100)
        driver.vechileTyrePressure= str(randint(50, 130))+'psi'
        driver.vechileSpeed=randint(1, 100)
        driver_data.append(driver)
    if len(driver_data) > 0:
        kafka_producer = connect_kafka_producer()
        for data in driver_data:
            publish_message(kafka_producer, 'topic-vehicle-data', 'raw',json.dumps(data.__dict__))
            sleep(5)
        if kafka_producer is not None:
            kafka_producer.close()




if __name__ == '__main__':
   
    while True:
        simulateVehicleData(1)
        sleep(2)
    
