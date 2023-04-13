from paho.mqtt import publish, subscribe
import threading
import json
import pickle
from datetime import datetime
import time
import random
import socket

MAIN_TOPIC = "PGL"
ALL_TOPICS = "PGL/#"
REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_event'
REQUEST_STORE_USER_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_user'
REQUEST_GET_EVENTS_TOPIC = f'{MAIN_TOPIC}/request/get_events'
REQUEST_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/request/valid_user'
REQUEST_CREATE_PRODUCT_TOPIC = f'{MAIN_TOPIC}/request/store_product'
RESPONSE_SEND_EVENTS_TOPIC = f'{MAIN_TOPIC}/response/send_events'
RESPONSE_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/response/valid_user'

REQUEST_NEW_DEVICE_TOPIC = f'{MAIN_TOPIC}/request/new_device'
user_request = "user2"

# t_end = time.time() + 60
# while time.time() < t_end:
#     now = datetime.now()
#     date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
#     r = random.randint(0,100)
#     t = random.randint(0,5)
#     publish.single("PGL/request/store_event", f"{date_time}; {r};{t};", hostname="localhost")

# give device number and user that should have access
publish.single(REQUEST_CREATE_PRODUCT_TOPIC, f"device3;user3;", hostname="localhost")

now = datetime.now()
date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
# publish.single(REQUEST_STORE_EVENT_IN_DB_TOPIC, f"{date_time};6237;93;{socket.gethostname()};")
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"{socket.gethostname()}")

# publish.single("PGL/request/get_events", f"{user_request}; ;", hostname="localhost")
# publish.single(REQUEST_VALIDATE_USER_TOPIC, "user1; pas1;", hostname="localhost")

# # create five user
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user1;pas1;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user2;pas2;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user3;pas3;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user4;pas4;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user5;pas5;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user6;pas6;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user7;pas7;user;", hostname="localhost")
# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user8;pas8;user;", hostname="localhost")

# #create five devices
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"device1", hostname="localhost")
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"device2", hostname="localhost")
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"device3", hostname="localhost")
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"device4", hostname="localhost")
# publish.single(REQUEST_NEW_DEVICE_TOPIC, f"device5", hostname="localhost")

# publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user2;sandboks123;user;", hostname="localhost")


# while True:
#     msg = subscribe.simple("PGL/response/send_events", hostname="localhost")
#     data = json.loads(msg.payload)
#     with open(f'data_{user_request}.txt', 'w') as f:
#         json.dump(data, f)
#     exit(0)

    