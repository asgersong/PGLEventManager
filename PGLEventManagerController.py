from paho.mqtt.client import Client as MqttClient, MQTTMessage
from threading import Event, Thread
from queue import Empty, Queue
import warnings

from PGLEventManagerModel import PGLEventManagerModel

class PGLEventManagerController:
    """The controller listens on MQTT topics and differentiates between three different. 
    Handles both incoming data to be stored in the database (model) as well as requests for 
    outgoing data (to the web server (?))"""

    # different MQTT topics
    # READ: Right now we publish on the '__RESPONSE_VALIDATE_USER_TOPIC' topic both when explicitly requested (logging in),
    # and when trying to create a new user (check for duplicates).
    __MAIN_TOPIC = "PGL"
    __ALL_TOPICS = "PGL/#"
    __REQUEST_TOPICS = f"{__MAIN_TOPIC}/request/#"
    # this is the events that the PI publishes to
    __REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{__MAIN_TOPIC}/request/store_event'
    __REQUEST_EMERGENCY_TOPIC = f'{__MAIN_TOPIC}/request/emergency'

    # these are the events that the web should request on
    __REQUEST_STORE_USER_IN_DB_TOPIC = f'{__MAIN_TOPIC}/request/store_user'
    __REQUEST_CREATE_PRODUCT_TOPIC = f'{__MAIN_TOPIC}/request/store_product'
    __REQUEST_GET_EVENTS_TOPIC = f'{__MAIN_TOPIC}/request/get_events'
    __REQUEST_VALIDATE_USER_TOPIC = f'{__MAIN_TOPIC}/request/valid_user'
    __REQUEST_NEW_DEVICE_TOPIC = f'{__MAIN_TOPIC}/request/new_device'

    __RESPONSE_SEND_EVENTS_TOPIC = f'{__MAIN_TOPIC}/response/send_events'
    __RESPONSE_VALIDATE_USER_TOPIC = f'{__MAIN_TOPIC}/response/valid_user'
    __RESPONSE_EMERGENCY_TOPIC = f'{__MAIN_TOPIC}/response/emergency'

    def __init__(self, mqtt_host: str, model: PGLEventManagerModel, mqtt_port: int = 1883) -> None:
        self.__subscriber_thread = Thread(target=self.__worker,
                                          daemon=True)
        self.__stop___worker = Event()
        self.__events_queue = Queue()
        self.__PGLmodel = model

        # mqtt parameters and callback methods
        self.__mqtt_host = mqtt_host
        self.__mqtt_port = mqtt_port
        self.__mqtt_client = MqttClient()
        self.__mqtt_client.on_message = self.__onMessage
        self.__mqtt_client.on_connect = self.__onConnect
        # initialize other callback methods here

    # connect the model to address and start the subscriber_thread
    def startListening(self) -> None:
        self.__PGLmodel.connectDB()  # connect to database

        self.__mqtt_client.connect(host=self.__mqtt_host,  # connect to mqtt
                                   port=self.__mqtt_port)

        self.__mqtt_client.loop_start()  # start loop
        self.__mqtt_client.subscribe(
            self.__REQUEST_TOPICS)  # subscribe to all topics
        self.__subscriber_thread.start()  # start subscriber thread (listens for mqtt)

    # stop subscriber thread and disconnect the model
    def stopListening(self) -> None:
        self.__stop___worker.set()
        self.__mqtt_client.loop_stop()
        self.__mqtt_client.unsubscribe(self.__ALL_TOPICS)
        self.__mqtt_client.disconnect()

        # Disconnect from the database
        self.__PGLmodel.disconnectDB()

    # callback method that is called when __mqtt_client is connected
    def __onConnect(self, client, userdata, flags, rc) -> None:
        print("MQTT client connected")

    # callback method that is called whenever a message arrives on a topic that '__mqtt_client' subscribes to
    def __onMessage(self, client, userdata, message: MQTTMessage) -> None:
        self.__events_queue.put(message)
        print(f'MQTT Message recievered with payload: {message.payload}')

    # __worker is the method that the __subscriber_thread runs
    # listens for MQTT events
    # empties __events_queue
    def __worker(self) -> None:
        print("Subscriber_thread __worker started")
        while not self.__stop___worker.is_set():
            try:
                # pull message form events queue
                # timeout indicates that we stop trying to dequeue after 1 s
                # throws 'Empty' exception if timeout
                mqtt_message: MQTTMessage = self.__events_queue.get(timeout=1)
            # if queue empty return
            except Empty:
                pass
            # if the pull was succesful, handle the message
            else:
                try:
                    mqtt_message_topic = mqtt_message.topic

                    if mqtt_message_topic == self.__REQUEST_NEW_DEVICE_TOPIC:
                        event_string = mqtt_message.payload.decode("utf-8")
                        self.__PGLmodel.store(
                            event_string, self.__PGLmodel.DEVICES_TABLE_NAME)

                    # store event from PI in database
                    elif mqtt_message_topic == self.__REQUEST_STORE_EVENT_IN_DB_TOPIC:
                        # if any logic should be computed on the incoming data, we should do it here?
                        event_string = mqtt_message.payload.decode("utf-8")
                        self.__PGLmodel.store(
                            event_string, self.__PGLmodel.JOURNEY_TABLE_NAME)

                    # store produc in database (from web request)
                    elif mqtt_message_topic == self.__REQUEST_CREATE_PRODUCT_TOPIC:
                        event_string = mqtt_message.payload.decode("utf-8")
                        self.__PGLmodel.store(
                            event_string, self.__PGLmodel.PRODUCT_TABLE_NAME)

                    # store user in database
                    elif mqtt_message_topic == self.__REQUEST_STORE_USER_IN_DB_TOPIC:
                        # if any logic should be computed on the incoming data, we should do it here
                        event_string = mqtt_message.payload.decode("utf-8")
                        succ = self.__PGLmodel.store(
                            event_string, self.__PGLmodel.USERS_TABLE_NAME)
                        # publish to indicate if user is stored succesfully
                        self.__mqtt_client.publish(
                            self.__RESPONSE_VALIDATE_USER_TOPIC, succ)

                    # return all events from database for given user
                    elif mqtt_message_topic == self.__REQUEST_GET_EVENTS_TOPIC:
                        # retrieve data from database using the model
                        credentials = mqtt_message.payload.decode("utf-8")
                        data = self.__PGLmodel.getEvents(
                            self.__PGLmodel.JOURNEY_TABLE_NAME, credentials)
                        # publish the data on the proper topic
                        self.__mqtt_client.publish(
                            self.__RESPONSE_SEND_EVENTS_TOPIC, data)
                        print("Published data")

                    # validate a user
                    elif mqtt_message_topic == self.__REQUEST_VALIDATE_USER_TOPIC:
                        credentials = mqtt_message.payload.decode("utf-8")
                        validity = self.__PGLmodel.getEvents(
                            self.__PGLmodel.USERS_TABLE_NAME, credentials)
                        self.__mqtt_client.publish(
                            self.__RESPONSE_VALIDATE_USER_TOPIC, validity)
                        print(f'Validated user: {validity}')

                    else:
                        # not the right topic
                        warnings.warn(
                            f'Message recieved on unkown topic: {mqtt_message.topic}')

                except KeyError:
                    print(f'Error occured in __worker: {KeyError}')
