import mysql.connector as mysql
import json


class PGLEventManagerModel:
    """Model to store timestamp events in mysql database.
    The model handles all interaction with the database. """

    # table names
    USERS_TABLE_NAME = "users"
    JOURNEY_TABLE_NAME = "journey"
    PRODUCT_TABLE_NAME = "products"
    DEVICES_TABLE_NAME = "devices"
    EMERGENCY_TABLE_NAME = "emergency"

    # table descriptions
    __USERS_TABLE_DESCRIPTION: str = """users 
                                       (user_id int NOT NULL AUTO_INCREMENT,
                                        username VARCHAR(320) UNIQUE NOT NULL,
                                        password VARCHAR(255) NOT NULL, 
                                        usertype VARCHAR(30) NOT NULL, 
                                        PRIMARY KEY(user_id) )"""

    __JOURNEY_TABLE_DESCRIPTION: str = """journey 
                                        (journey_id int NOT NULL AUTO_INCREMENT, 
                                        datetime VARCHAR(30) NOT NULL, 
                                        rtt VARCHAR(30) NOT NULL, 
                                        tt  VARCHAR(30),
                                        device_id VARCHAR(255) NOT NULL,
                                        PRIMARY KEY (journey_id),
                                        FOREIGN KEY (device_id) REFERENCES devices(device_id))"""
    
    __EMERGENCY_TABLE_DESCRIPTION: str = """emergency
                                        (emergency_id int NOT NULL AUTO_INCREMENT,
                                        datetime VARCHAR(30) NOT NULL,
                                        et VARCHAR(30) NOT NULL,
                                        device_id VARCHAR(255) NOT NULL,
                                        PRIMARY KEY (emergency_id),
                                        FOREIGN KEY (device_id) REFERENCES devices(device_id))"""

    __PRODUCTS_TABLE_DESCRIPTION: str = """products (
                                            device_id VARCHAR(255) NOT NULL,
                                            user_id int NOT NULL,
                                            PRIMARY KEY (device_id, user_id),
                                            FOREIGN KEY (device_id) REFERENCES devices(device_id),
                                            FOREIGN KEY (user_id) REFERENCES users(user_id))"""

    __DEVICES_TABLE_DESCRIPTION: str = """devices
                                          (device_id VARCHAR(255) NOT NULL,
                                          PRIMARY KEY (device_id)) """

    def __init__(self, host, database: str, user: str, password: str) -> None:
        self.__host = host
        self.__database_name = database
        self.__user = user
        self.__password = password
        self.__PGL_db_connection = None

    def connectDB(self) -> None:
        # establish database connection
        try:
            self.__PGL_db_connection = mysql.connect(host=self.__host,
                                                     user=self.__user,
                                                     password=self.__password)

            self.__PGL_db_connection.cursor().execute(
                f"USE {self.__database_name}")
            print("Connected to database succesfully")

        except mysql.Error as err:
            # If the database doesn't exist, then create it.
            if err.errno == mysql.errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist. Will be created.")
                self.__createDatabase()
                print(f"Database {self.__database_name} created successfully.")
            else:
                print(f'Failed connecting to database with error: {err}')

    # disconnect from the database
    def disconnectDB(self) -> None:
        self.__PGL_db_connection.disconnect()
        print("Disconnected from database")

    # creates database with parameters from __init__
    def __createDatabase(self) -> None:
        cursor = self.__PGL_db_connection.cursor()

        try:
            # create database
            cursor.execute(
                f"CREATE DATABASE {self.__database_name} DEFAULT CHARACTER SET 'utf8'")
        except mysql.Error as err:
            # catch error
            print(f"Failed to create database with error: {err}")

        else:
            # move cursor to work in this database
            cursor.execute(f'USE {self.__database_name}')
            cursor.execute(f"CREATE TABLE {self.__USERS_TABLE_DESCRIPTION}")
            cursor.execute(f"CREATE TABLE {self.__DEVICES_TABLE_DESCRIPTION}")
            cursor.execute(f"CREATE TABLE {self.__PRODUCTS_TABLE_DESCRIPTION}")
            cursor.execute(f"CREATE TABLE {self.__JOURNEY_TABLE_DESCRIPTION}")
            cursor.execute(f"CREATE TABLE {self.__EMERGENCY_TABLE_DESCRIPTION}")

        cursor.close()
        self.__PGL_db_connection = self.__database_name

    def __userExists(self, username) -> bool:
        cursor = self.__PGL_db_connection.cursor()
        query = f'SELECT COUNT(username) FROM {self.USERS_TABLE_NAME} WHERE username = "{username}";'
        cursor.execute(query)
        duplicates = cursor.fetchone()[0]
        if duplicates == 0:
            return False
        else:
            return True

    # store event in database.
    # event is in string format with entry values separated by ';'

    def store(self, event: str, table: str):
        # we should format the event here in respective columns and such
        try:
            match table:
                # store new device
                case self.DEVICES_TABLE_NAME:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f'INSERT INTO {self.DEVICES_TABLE_NAME} (device_id) VALUES ("{event}")'
                    cursor.execute(query)
                    self.__PGL_db_connection.commit()
                    print("Stored device in DB")

                # store timestamp event in 'events' table
                case self.JOURNEY_TABLE_NAME:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"INSERT INTO {self.JOURNEY_TABLE_NAME} (datetime, rtt, tt, device_id) VALUES (%s, %s, %s, %s)"
                    val = tuple(event.split(';')[:-1])
                    cursor.execute(query, val)
                    self.__PGL_db_connection.commit()
                    print("Stored event in DB")


                # store emergency event in 'emergenct' table
                case self.EMERGENCY_TABLE_NAME:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"INSERT INTO {self.EMERGENCY_TABLE_NAME} (datetime, et, device_id) VALUES (%s, %s, %s)"
                    val = tuple(event.split(';')[:-1])
                    cursor.execute(query, val)
                    self.__PGL_db_connection.commit()
                    print("Stored emergency in DB")

                # store user in 'users' table
                case self.USERS_TABLE_NAME:
                    cursor = self.__PGL_db_connection.cursor()
                    # check that user doesn't already exist
                    val = tuple(event.split(';')[:-1])
                    # if no duplicates, insert in table
                    if not self.__userExists(val[0]):
                        cursor.reset()
                        query = f"INSERT INTO {self.USERS_TABLE_NAME} (username, password, usertype) VALUES (%s, %s, %s)"
                        cursor.execute(query, val)
                        self.__PGL_db_connection.commit()
                        print("Stored user in DB")
                        return 'VALID'

                    # user already exists
                    else:
                        cursor.reset()
                        cursor.close()
                        print("Duplicate user not stored")
                        return 'INVALID'

                # store new 'product' in products table
                case self.PRODUCT_TABLE_NAME:
                    cursor = self.__PGL_db_connection.cursor()
                    val = tuple(event.split(';')[:-1])
                    user = val[1]
                    device_id = val[0]
                    query = f"""INSERT INTO products (device_id, user_id) 
                                    VALUES ('{device_id}', 
                                        (SELECT user_id FROM users WHERE username = '{user}'));"""

                    cursor.execute(query)
                    self.__PGL_db_connection.commit()
                    print(f'Insert user: {user} and device_id: {device_id}')

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

        cursor.close()
    
    def __eventsToJson(self, data) -> str:
        events = []
        for row in data:
            events.append(row)
        events_json = json.dumps(events)
        return events_json

    def getEvents(self, table: str, payload_in: str) -> str:

        match table:
            # returns all data related to the query from the database as string in json format
            case self.JOURNEY_TABLE_NAME:
                payload_in = tuple(payload_in.split(';')[:-1])  # get payload as tuple
                username = payload_in[0] # get username from payload

                if len(payload_in) > 1: device_id = payload_in[1]  # get device_id from payload if available
                else: device_id = 0

                # return ALL data related to user. Returns empty list if no data
                if device_id == 0:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"""SELECT * FROM {self.JOURNEY_TABLE_NAME} 
                                    JOIN {self.PRODUCT_TABLE_NAME} ON journey.device_id = products.device_id 
                                        WHERE products.user_id = 
                                            (SELECT user_id FROM {self.USERS_TABLE_NAME} 
                                                WHERE username = '{username}')"""
                    cursor.execute(query)
                    all_data = cursor.fetchall()
                    return self.__eventsToJson(all_data)

                # return data related to specific device and user. Returns empty list if no data
                elif device_id != 0:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"""SELECT * FROM {self.JOURNEY_TABLE_NAME} 
                                    JOIN {self.PRODUCT_TABLE_NAME} ON journey.device_id = products.device_id 
                                        WHERE products.user_id = 
                                            (SELECT user_id FROM {self.USERS_TABLE_NAME} WHERE username = '{username}') 
                                                AND products.device_id = '{device_id}'"""
                    cursor.execute(query)
                    all_data = cursor.fetchall()
                    return self.__eventsToJson(all_data)
                
            case self.EMERGENCY_TABLE_NAME:
                payload_in = tuple(payload_in.split(';')[:-1])  # get payload as tuple
                username = payload_in[0]                        # get username from payload

                if len(payload_in) > 1: device_id = payload_in[1]  # get device_id from payload if available
                else: device_id = 0

                # return ALL emergencies related to user. Returns empty list if no data
                if device_id == 0:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"""SELECT * FROM {self.EMERGENCY_TABLE_NAME}
                                    JOIN {self.PRODUCT_TABLE_NAME} ON emergency.device_id = products.device_id
                                        WHERE products.user_id =
                                            (SELECT user_id FROM {self.USERS_TABLE_NAME} 
                                                WHERE username = '{username}')"""
                    
                    cursor.execute(query)
                    all_data = cursor.fetchall()
                    return self.__eventsToJson(all_data)

                elif device_id != 0:
                    cursor = self.__PGL_db_connection.cursor()
                    query = f"""SELECT * FROM {self.EMERGENCY_TABLE_NAME} 
                                    JOIN {self.PRODUCT_TABLE_NAME} ON journey.device_id = products.device_id 
                                        WHERE products.user_id = 
                                            (SELECT user_id FROM {self.USERS_TABLE_NAME} WHERE username = '{username}') 
                                                AND products.device_id = '{device_id}'"""
                    cursor.execute(query)
                    all_data = cursor.fetchall()
                    return self.__eventsToJson(all_data)         

            # validates a user by checking if the user/pass combination exists in 'users' table
            case self.USERS_TABLE_NAME:
                payload_in = tuple(payload_in.split(';')[:-1])
                user = payload_in[0]
                pass_ = payload_in[1]
                cursor = self.__PGL_db_connection.cursor()
                query = f'SELECT COUNT(*) FROM {self.USERS_TABLE_NAME} WHERE username = "{user}" AND password = "{pass_}"'

                try:
                    cursor.execute(query)
                    rows = cursor.fetchone()
                    count = rows[0]
                    cursor.reset()
                    cursor.close()
                    if (count > 0):
                        return 'VALID'
                    else:
                        return 'INVALID'

                except mysql.Error as err:
                    Warning.warn("Failed to validate user")
                    return 'INVALID'
