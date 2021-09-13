from Models import Driver
from time import sleep
from random import randint
import random
from faker import Faker
import json

import requests
from bs4 import BeautifulSoup
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

def input_data(x): 
  
    # dictionary 
    driver_data =[] 
    vehcle_list = ["Tata Pickup", "Tata Turbo xls",
             "Eicher Motor Turbo", "Gati Transport"]
    state_list = ["UP", "BR",
             "DL", "KA","UK"]
    fake = Faker()
    for i in range(0, x): 
        driver= Driver() 
        driver.id= randint(1, 100) 
        driver.driverName= fake.name() 
        driver.driverTempreture= str(randint(20,60))+'F'
        driver.driverBloodPressure= str(randint(0,200))+'/'+ str(randint(60,90))+'mmHg'
        driver.address= fake.address() 
        driver.latitude= str(fake.latitude()) 
        driver.longitude= str(fake.longitude()) 
        driver.vechileModel= str(random.choice(vehcle_list))
        driver.vechileLiecense= str(random.choice(vehcle_list))+'-'+str(randint(10, 99))+'-'+str(randint(1000, 9999)) # UP-60-9999
        driver.vechileTempreture= str(randint(30, 150))+'F' 
        driver.vechileFuelLevel= str(randint(1, 100))+'%'
        driver.vechileTyrePressure= str(randint(50, 130))+'psi'
        driver_data.append(driver)
    return driver_data


def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()



if __name__ == '__main__':
   

    #all_recipes = get_recipes()
    driver_data=input_data(100)
    if len(driver_data) > 0:
        kafka_producer = connect_kafka_producer()
        for data in driver_data:
            publish_message(data, 'raw_recipes', 'raw',json.dumps(data))
        if kafka_producer is not None:
            kafka_producer.close()
