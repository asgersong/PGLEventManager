from paho.mqtt import publish, subscribe
import threading
import json
import pickle
from datetime import datetime
import time
import random
import socket

hostname = "test.mosquitto.org"

__MAIN_TOPIC = "PGL"
REQUEST_TOPICS = f"{__MAIN_TOPIC}/request/#"
# this is the events that the PI publishes to
REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{__MAIN_TOPIC}/request/store_event'
REQUEST_EMERGENCY_TOPIC = f'{__MAIN_TOPIC}/request/emergency'
# these are the events that the web should request on
REQUEST_STORE_USER_IN_DB_TOPIC = f'{__MAIN_TOPIC}/request/store_user'
REQUEST_CREATE_PRODUCT_TOPIC = f'{__MAIN_TOPIC}/request/store_product'
REQUEST_GET_EVENTS_TOPIC = f'{__MAIN_TOPIC}/request/get_events'
REQUEST_GET_EMERGENCIES_TOPIC = f'{__MAIN_TOPIC}/request/get_emergencies'
REQUEST_VALIDATE_USER_TOPIC = f'{__MAIN_TOPIC}/request/valid_user'
REQUEST_NEW_DEVICE_TOPIC = f'{__MAIN_TOPIC}/request/new_device'
RESPONSE_SEND_EVENTS_TOPIC = f'{__MAIN_TOPIC}/response/send_events'
RESPONSE_VALIDATE_USER_TOPIC = f'{__MAIN_TOPIC}/response/valid_user'
RESPONSE_EMERGENCY_TOPIC = f'{__MAIN_TOPIC}/response/emergency'

user_request = "user1"

# method to create user
def create_user(user, pass_):

    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_VALIDATE_USER_TOPIC, hostname=hostname)
            resp = msg.payload.decode("utf-8")
            print(resp)
            exit(0)

    t = threading.Thread(target=listen_response)
    publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, f"{user};{pass_};user;", hostname=hostname)
    t.start()
    time.sleep(2)
    t.join()


# method to create admin
def create_admin(user, pass_):
    publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, f"{user};{pass_};admin;", hostname=hostname)

# method to create device
def create_device(device):
    publish.single(REQUEST_NEW_DEVICE_TOPIC, f"{device}", hostname=hostname)

# method to create product
def create_product(device, user):
    publish.single(REQUEST_CREATE_PRODUCT_TOPIC, f"{device};{user};", hostname=hostname)

# method to create event
def create_event(device):
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
    r = random.randint(0, 10000)
    t = random.randint(0, 3000)
    publish.single(REQUEST_STORE_EVENT_IN_DB_TOPIC, f"{date_time};{r};{t};{device};", hostname=hostname)

# method to get events
def get_events(user):

    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_SEND_EVENTS_TOPIC, hostname=hostname)
            data = json.loads(msg.payload)
            with open(f'testfiles/events_{user}.json', 'w') as f:
                json.dump(data, f)
            exit(0)
    
    t = threading.Thread(target=listen_response)
    t.start()
    publish.single(REQUEST_GET_EVENTS_TOPIC, f"{user};", hostname=hostname)
    time.sleep(2)
    t.join()


# method to get events to user and device
def get_events_to_user_and_device(user, device):

    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_SEND_EVENTS_TOPIC, hostname=hostname)
            data = json.loads(msg.payload)
            with open(f'testfiles/events_{device}_{user}.json', 'w') as f:
                json.dump(data, f)
            exit(0)

    t = threading.Thread(target=listen_response)
    t.start()
    publish.single(REQUEST_GET_EVENTS_TOPIC, f"{user};{device};", hostname=hostname)
    time.sleep(2)
    t.join()

# method to get emergencies
def get_emergencies(user):

    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_EMERGENCY_TOPIC, hostname=hostname)
            data = json.loads(msg.payload)
            with open(f'testfiles/emergencies_{user}.json', 'w') as f:
                json.dump(data, f)
            exit(0)

    t = threading.Thread(target=listen_response)
    t.start()
    publish.single(REQUEST_GET_EMERGENCIES_TOPIC, f"{user};", hostname=hostname)
    time.sleep(2)
    t.join()

# method to get emergenvies to user and device
def get_emergencies_to_user_and_device(user, device):

    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_EMERGENCY_TOPIC, hostname=hostname)
            data = json.loads(msg.payload)
            with open(f'testfiles/emergencies_{device}_{user}.json', 'w') as f:
                json.dump(data, f)
            exit(0)

    t = threading.Thread(target=listen_response)
    t.start()
    publish.single(REQUEST_GET_EMERGENCIES_TOPIC, f"{user};{device};", hostname=hostname)
    time.sleep(2)
    t.join()


# method to validate user
def validate_user(user, pass_):
    def listen_response():
        while True:
            msg = subscribe.simple(RESPONSE_VALIDATE_USER_TOPIC, hostname=hostname)
            resp = msg.payload.decode("utf-8")
            print(resp)
            exit(0)

    t = threading.Thread(target=listen_response)
    t.start()
    publish.single(REQUEST_VALIDATE_USER_TOPIC, f"{user};{pass_};", hostname=hostname)
    time.sleep(2)
    t.join()



# create_event("device1")
# get_events("user1")
# validate_user("user1", "pass1")

create_user("user1", "pass1")