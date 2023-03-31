from paho.mqtt import publish, subscribe
import threading
import json

MAIN_TOPIC = "PGL"
ALL_TOPICS = "PGL/#"
REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_event'
REQUEST_STORE_USER_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_user'
REQUEST_GET_EVENTS_TOPIC = f'{MAIN_TOPIC}/request/get_events'
REQUEST_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/request/valid_user'

RESPONSE_SEND_EVENTS_TOPIC = f'{MAIN_TOPIC}/response/send_events'
RESPONSE_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/response/valid_user'

# publish.single("PGL/request/store_event", "43; PIR_01;", hostname="localhost")
# publish.single("PGL/request/get_events", hostname="localhost")
publish.single(REQUEST_VALIDATE_USER_TOPIC, "user1; pas1;", hostname="localhost")



# stopper = threading.Event()

# while True:
#     msg = subscribe.simple("PGL/response/send_events", hostname="localhost")
#     print(json.loads(msg.payload))
    