from paho.mqtt import publish, subscribe
import threading
import json
import pickle
from datetime import datetime
import time
import random

MAIN_TOPIC = "PGL"
ALL_TOPICS = "PGL/#"
REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_event'
REQUEST_STORE_USER_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_user'
REQUEST_GET_EVENTS_TOPIC = f'{MAIN_TOPIC}/request/get_events'
REQUEST_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/request/valid_user'
REQUEST_CREATE_PRODUCT_TOPIC = f'{MAIN_TOPIC}/request/store_product'
RESPONSE_SEND_EVENTS_TOPIC = f'{MAIN_TOPIC}/response/send_events'
RESPONSE_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/response/valid_user'

t_end = time.time() + 60
# while time.time() < t_end:
#     now = datetime.now()
#     date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
#     r = random.randint(0,100)
#     t = random.randint(0,5)
#     publish.single("PGL/request/store_event", f"{date_time}; {r};{t};", hostname="localhost")

# give device number and user that should have access
# publish.single(REQUEST_CREATE_PRODUCT_TOPIC, "4;sandkassesandx@gmail.com;")

# publish.single("PGL/request/get_events", "user1; ;", hostname="localhost")
# publish.single(REQUEST_VALIDATE_USER_TOPIC, "user1; pas1;", hostname="localhost")
publish.single(REQUEST_STORE_USER_IN_DB_TOPIC, "user3;sandkasse123;user;", hostname="localhost")


# while True:
#     msg = subscribe.simple("PGL/response/send_events", hostname="localhost")
#     data = json.loads(msg.payload)
#     with open('data.txt', 'w') as f:
#         json.dump(data, f)

    