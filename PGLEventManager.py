from queue import Empty, Queue
import mysql.connector as mysql
from paho.mqtt.client import Client as MqttClient, MQTTMessage
import warnings
import json
from threading import Event, Thread

class PGLEventManagerModel:
    """Model to store sensor events in mysql database.
    The model handles all interaction with the database. """

    USERS_TABLE_NAME = 'users'
    EVENTS_TABLE_NAME = 'events'

    def __init__(self, host, database: str, user: str, password: str) -> None:
        self.__host = host
        self.__database_name = database
        self.__user = user
        self.__password = password
        self.__PGL_db_connection = None

    def connectDB(self) -> None:
        # establish database connection
        try:
            self.__PGL_db_connection = mysql.connect(host = self.__host,
                                                     user = self.__user,
                                                     password = self.__password)
            
            self.__PGL_db_connection.cursor().execute(f"USE {self.__database_name}")
            print("Connected to database succesfully")

        except mysql.Error as err:
            # If the database doesn't exist, then create it.
            if err.errno == mysql.errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist. Will be created.")
                self.createDatabase()
                print(f"Database {self.__database_name} created successfully.")
                self.__PGL_db_connection.database = self.__database_name
            
            else:
                print(f'Failed connecting to database with error: {err}')

    # disconnect from the database
    def disconnectDB(self) -> None:
        self.__PGL_db_connection.disconnect()

    # creates database with parameters from __init__
    def createDatabase(self) -> None:
        cursor = self.__PGL_db_connection.cursor()

        try:
            cursor.execute(f"CREATE DATABASE {self.__database_name} DEFAULT CHARACTER SET 'utf8'")  #create database
        except mysql.Error as err:
            print(f"Failed to create database with error: {err}")                                   #catch error

        else:
            cursor.execute(f'USE {self.__database_name}')                                           #move cursor to work in this database
            cursor.execute(f"CREATE TABLE {self.EVENTS_TABLE_NAME} (timestamp VARCHAR(30), sensor VARCHAR(30))")       #create events table with two columns
            cursor.execute(f"CREATE TABLE {self.USERS_TABLE_NAME} (username VARCHAR(320), password VARCHAR(255))")     #create users table with two columns

        cursor.close()
        self.__PGL_db_connection = self.__database_name
                           

    # store event in database.
    # event is in string format with entry values separated by ';'
    def store(self, event, table : str):
        # we should format the event here in respective columns and such
        try:
            # store sensor event in 'events' table
            if table == self.EVENTS_TABLE_NAME:
                cursor = self.__PGL_db_connection.cursor()
                query = f"INSERT INTO {self.EVENTS_TABLE_NAME} (timestamp, sensor) VALUES (%s, %s)"
                val = tuple(event.split(';')[:-1])
                cursor.execute(query, val)
                self.__PGL_db_connection.commit()
                print("Stored event in DB")

            # store user in 'users' table
            elif table == self.USERS_TABLE_NAME:
                cursor = self.__PGL_db_connection.cursor()

                # check that user doesn't already exist
                val = tuple(event.split(';')[:-1])
                query = f'SELECT COUNT(username) FROM {self.USERS_TABLE_NAME} WHERE username = "{val[0]}";'
                cursor.execute(query)
                duplicates = cursor.fetchone()[0]
                
                # if no duplicates, insert in table
                if duplicates == 0:
                    cursor.reset()
                    query = f"INSERT INTO {self.USERS_TABLE_NAME} (username, password) VALUES (%s, %s)"
                    cursor.execute(query, val)
                    self.__PGL_db_connection.commit()
                    print("Stored user in DB")
                    return 'VALID'

                # user already exists
                else:
                    cursor.reset()
                    cursor.close()
                    return 'INVALID'

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

        cursor.close()

    
    def getEvents(self, table : str, credentials : str) -> str:
        # returns all data from the database as string in json format
        if table == self.EVENTS_TABLE_NAME:
            query = f"SELECT * FROM {self.EVENTS_TABLE_NAME}"
            cursor = self.__PGL_db_connection.cursor()
            events = []
            cursor.execute(query)
            all_data = cursor.fetchall()

            for row in all_data:
                events.append(row)

            events_json = json.dumps(events)
            return events_json
        
        # validates a user by checking if the user/pass combination exists in 'users' table
        elif table == self.USERS_TABLE_NAME:
            credentials = tuple(credentials.split(';')[:-1])
            user = credentials[0]
            pass_ = credentials[1] 
            cursor = self.__PGL_db_connection.cursor()
            query = f'SELECT COUNT(*) FROM {self.USERS_TABLE_NAME} WHERE username = "{user}" AND password = "{pass_}"'

            try:
                cursor.execute(query)
                
                if cursor.fetchone()[0] > 0:
                    return 'VALID'
                else:
                    return 'INVALID'
            
            except mysql.Error as err:
                Warning.warn("Failed to validate user")                


class PGLEventManagerController:
    """The controller listens on MQTT topics and differentiates between three different. 
    Handles both incoming data to be stored in the database (model) as well as requests for 
    outgoing data (to the web server (?))"""

    # different MQTT topics
    # READ: Right now we publish on the 'RESPONSE_VALIDATE_USER_TOPIC' topic both when explicitly requested (logging in),
    # and when trying to create a new user (check for duplicates). 
    MAIN_TOPIC = "PGL"
    ALL_TOPICS = "PGL/#"
    REQUEST_TOPICS = f"{MAIN_TOPIC}/request/#"
    #this is the only event that the PI publishes to
    REQUEST_STORE_EVENT_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_event'   

    # these are the events that the web should request on
    REQUEST_STORE_USER_IN_DB_TOPIC = f'{MAIN_TOPIC}/request/store_user'
    REQUEST_GET_EVENTS_TOPIC = f'{MAIN_TOPIC}/request/get_events'
    REQUEST_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/request/valid_user'

    RESPONSE_SEND_EVENTS_TOPIC = f'{MAIN_TOPIC}/response/send_events'
    RESPONSE_VALIDATE_USER_TOPIC = f'{MAIN_TOPIC}/response/valid_user'

    def __init__(self, mqtt_host:str, model: PGLEventManagerModel,mqtt_port: int = 1883) -> None:
        self.__subscriber_thread = Thread(target=self.worker,
                                          daemon=True)
        self.__stop_worker = Event()
        self.__events_queue = Queue()
        self.__PGLmodel = model

        # mqtt parameters and callback methods
        self.__mqtt_host = mqtt_host
        self.__mqtt_port = mqtt_port
        self.__mqtt_client = MqttClient()
        self.__mqtt_client.on_message = self.onMessage
        self.__mqtt_client.on_connect = self.onConnect
        # initialize other callback methods here

    # connect the model to address and start the subscriber_thread
    def startListening(self) -> None:
        self.__PGLmodel.connectDB()                                 #connect to database

        self.__mqtt_client.connect(host=self.__mqtt_host,           #connect to mqtt
                                   port=self.__mqtt_port)
        
        self.__mqtt_client.loop_start()                             #start loop
        self.__mqtt_client.subscribe(self.REQUEST_TOPICS)           #subscribe to all topics
        self.__subscriber_thread.start()                            #start subscriber thread (listens for mqtt)

    # stop subscriber thread and disconnect the model
    def stopListening(self) -> None:
        self.__stop_worker.set()
        self.__mqtt_client.loop_stop()
        self.__mqtt_client.unsubscribe(self.ALL_TOPICS)
        self.__mqtt_client.disconnect()

        # Disconnect from the database
        self.__model.disconnect()

    # callback method that is called when __mqtt_client is connected
    def onConnect(self, client, userdata, flags, rc) -> None:
        print("MQTT client connected")

    # callback method that is called whenever a message arrives on a topic that '__mqtt_client' subscribes to
    def onMessage(self, client, userdata, message : MQTTMessage) -> None:
        self.__events_queue.put(message)
        print(f'MQTT Message recievered with payload: {message.payload}')

    # worker is the method that the __subscriber_thread runs
    # listens for MQTT events
    # empties __events_queue
    def worker(self) -> None:
        print("Subscriber_thread worker started")
        while not self.__stop_worker.is_set():
            try:
                # pull message form events queue
                # timeout indicates that we stop trying to dequeue after 1 s
                # throws 'Empty' exception if timeout
                mqtt_message : MQTTMessage = self.__events_queue.get(timeout = 1)
            # if queue empty return
            except Empty:
                pass
            # if the pull was succesful, handle the message
            else:
                try:
                    # store event from PI in database
                    if mqtt_message.topic == self.REQUEST_STORE_EVENT_IN_DB_TOPIC:
                        # if any logic should be computed on the incoming data, we should do it here
                        event_string = mqtt_message.payload.decode("utf-8")
                        self.__PGLmodel.store(event_string, self.__PGLmodel.EVENTS_TABLE_NAME)

                    # store user in database
                    elif mqtt_message.topic == self.REQUEST_STORE_USER_IN_DB_TOPIC:
                        # if any logic should be computed on the incoming data, we should do it here
                        event_string = mqtt_message.payload.decode("utf-8")
                        succ = self.__PGLmodel.store(event_string, self.__PGLmodel.USERS_TABLE_NAME)
                        self.__mqtt_client.publish(self.RESPONSE_VALIDATE_USER_TOPIC, succ)
                        print(f'Stored user: {succ}')

                    # return all events from database
                    elif mqtt_message.topic == self.REQUEST_GET_EVENTS_TOPIC:
                        # retrieve data from database using the model
                        data = self.__PGLmodel.getEvents(self.__PGLmodel.EVENTS_TABLE_NAME)
                        # publish the data on the proper topic
                        self.__mqtt_client.publish(self.RESPONSE_SEND_EVENTS_TOPIC, data)
                        print("Published data")

                    # validate a user
                    elif mqtt_message.topic == self.REQUEST_VALIDATE_USER_TOPIC:
                        credentials = mqtt_message.payload.decode("utf-8")
                        validity = self.__PGLmodel.getEvents(self.__PGLmodel.USERS_TABLE_NAME, credentials)
                        self.__mqtt_client.publish(self.RESPONSE_VALIDATE_USER_TOPIC, validity)
                        print(f'Validated user: {validity}')

                    else:
                        # not the right topic
                        warnings.warn(f'Message recieved on unkown topic: {mqtt_message.topic}')

                except KeyError:
                    print(f'Error occured in worker: {KeyError}')


def main():
    stop_daemon = Event()

    model = PGLEventManagerModel("localhost", "PGL", "PGL", "PGL")
    controller = PGLEventManagerController("localhost", model)

    controller.startListening()

    while not stop_daemon.is_set():
        stop_daemon.wait(60)

    controller.stopListening()


if __name__ == "__main__":
    main()