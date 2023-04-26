import mysql.connector as mysql
import json


class PGLEventManagerModel:
    """Model to store timestamp events in mysql database.
    The model handles all interaction with the database. """

    # table names
    __USERS_TABLE_NAME = "users"
    __JOURNEY_TABLE_NAME = "journey"
    __PRODUCT_TABLE_NAME = "products"
    __DEVICES_TABLE_NAME = "devices"
    __EMERGENCY_TABLE_NAME = "emergency"

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

    __TABLE_DESCRIPTIONS = [__USERS_TABLE_DESCRIPTION, __JOURNEY_TABLE_DESCRIPTION,
                            __PRODUCTS_TABLE_DESCRIPTION, __DEVICES_TABLE_DESCRIPTION, __EMERGENCY_TABLE_DESCRIPTION]

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

            # create tables
            for table in self.__TABLE_DESCRIPTIONS:
                cursor.execute(f"CREATE TABLE {table}")

        cursor.close()
        self.__PGL_db_connection = self.__database_name

    def __userExists(self, username) -> bool:
        cursor = self.__PGL_db_connection.cursor()
        query = f'SELECT COUNT(username) FROM {self.__USERS_TABLE_NAME} WHERE username = "{username}";'
        cursor.execute(query)
        duplicates = cursor.fetchone()[0]
        if duplicates == 0:
            return False
        else:
            return True

    def __deviceExists(self, device_id) -> bool:
        cursor = self.__PGL_db_connection.cursor()
        query = f'SELECT COUNT(device_id) FROM {self.__DEVICES_TABLE_NAME} WHERE device_id = "{device_id}";'
        cursor.execute(query)
        duplicates = cursor.fetchone()[0]
        if duplicates == 0:
            return False
        else:
            return True

# region Store data in database
    def storeDevice(self, device_id: str) -> None:
        try:
            cursor = self.__PGL_db_connection.cursor()
            query = f'INSERT INTO {self.__DEVICES_TABLE_NAME} (device_id) VALUES ("{device_id}")'
            cursor.execute(query)
            self.__PGL_db_connection.commit()
            print("Stored device in DB")
            cursor.close()

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

    def storeJourney(self, payload: str) -> None:
        try:
            cursor = self.__PGL_db_connection.cursor()
            query = f"INSERT INTO {self.__JOURNEY_TABLE_NAME} (datetime, rtt, tt, device_id) VALUES (%s, %s, %s, %s)"
            val = tuple(payload.split(';')[:-1])

            device = val[3]
            if not self.__deviceExists(device):
                print(f"Device: {device} does not exist in DB. Will be created.")
                self.storeDevice(device)

            cursor.execute(query, val)
            self.__PGL_db_connection.commit()
            print("Stored event in DB")
            cursor.close()

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

    def storeEmergency(self, payload: str) -> None:
        try:
            cursor = self.__PGL_db_connection.cursor()
            query = f"INSERT INTO {self.__EMERGENCY_TABLE_NAME} (datetime, et, device_id) VALUES (%s, %s, %s)"
            val = tuple(payload.split(';')[:-1])
            cursor.execute(query, val)
            self.__PGL_db_connection.commit()
            print("Stored emergency in DB")
            cursor.close()

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

    def storeUser(self, credentials: str) -> None:
        try:
            cursor = self.__PGL_db_connection.cursor()
            val = tuple(credentials.split(';')[:-1])

            # if no duplicates, insert in table
            if not self.__userExists(val[0]):
                cursor.reset()
                query = f"INSERT INTO {self.__USERS_TABLE_NAME} (username, password, usertype) VALUES (%s, %s, %s)"
                cursor.execute(query, val)
                self.__PGL_db_connection.commit()
                print("Stored user in DB")
                cursor.close()
                return 'VALID'

            # user already exists
            else:
                cursor.reset()
                cursor.close()
                print("Duplicate user not stored")
                return 'INVALID'

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

    def storeProduct(self, payload: str) -> None:
        try:
            cursor = self.__PGL_db_connection.cursor()
            val = tuple(payload.split(';')[:-1])
            user = val[1]
            device_id = val[0]
            query = f"""INSERT INTO products (device_id, user_id) 
                            VALUES ('{device_id}', 
                                (SELECT user_id FROM users WHERE username = '{user}'));"""

            cursor.execute(query)
            self.__PGL_db_connection.commit()
            print(f'Insert user: {user} and device_id: {device_id}')
            cursor.close()

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

# endregion

    def __eventsToJson(self, data) -> str:
        events = []
        for row in data:
            events.append(row)
        events_json = json.dumps(events)
        return events_json

# region Get data from database

    def getJourneys(self, payload: str) -> str:
        payload_in = tuple(payload.split(';')[:-1])  # get payload as tuple
        username = payload_in[0]  # get username from payload

        if len(payload_in) > 1:
            # get device_id from payload if available
            device_id = payload_in[1]
        else:
            device_id = 0

        # return ALL data related to user. Returns empty list if no data
        if device_id == 0:
            cursor = self.__PGL_db_connection.cursor()
            query = f"""SELECT * FROM {self.__JOURNEY_TABLE_NAME} 
                                JOIN {self.__PRODUCT_TABLE_NAME} ON journey.device_id = products.device_id 
                                    WHERE products.user_id = 
                                        (SELECT user_id FROM {self.__USERS_TABLE_NAME} 
                                            WHERE username = '{username}')"""
            cursor.execute(query)
            all_data = cursor.fetchall()
            return self.__eventsToJson(all_data)

        # return data related to specific device and user. Returns empty list if no data
        elif device_id != 0:
            cursor = self.__PGL_db_connection.cursor()
            query = f"""SELECT * FROM {self.__JOURNEY_TABLE_NAME} 
                                JOIN {self.__PRODUCT_TABLE_NAME} ON journey.device_id = products.device_id 
                                    WHERE products.user_id = 
                                        (SELECT user_id FROM {self.__USERS_TABLE_NAME} WHERE username = '{username}') 
                                            AND products.device_id = '{device_id}'"""
            cursor.execute(query)
            all_data = cursor.fetchall()
            return self.__eventsToJson(all_data)

    def getEmergencies(self, payload: str) -> str:
        payload_in = tuple(payload.split(';')[:-1])  # get payload as tuple
        # get username from payload
        username = payload_in[0]

        if len(payload_in) > 1:
            # get device_id from payload if available
            device_id = payload_in[1]
        else:
            device_id = 0

        # return ALL emergencies related to user. Returns empty list if no data
        if device_id == 0:
            cursor = self.__PGL_db_connection.cursor()
            query = f"""SELECT * FROM {self.__EMERGENCY_TABLE_NAME}
                            JOIN {self.__PRODUCT_TABLE_NAME} ON emergency.device_id = products.device_id
                                WHERE products.user_id =
                                    (SELECT user_id FROM {self.__USERS_TABLE_NAME} 
                                        WHERE username = '{username}')"""

            cursor.execute(query)
            all_data = cursor.fetchall()
            return self.__eventsToJson(all_data)

        elif device_id != 0:
            cursor = self.__PGL_db_connection.cursor()
            query = f"""SELECT * FROM {self.__EMERGENCY_TABLE_NAME} 
                            JOIN {self.__PRODUCT_TABLE_NAME} ON emergency.device_id = products.device_id 
                                WHERE products.user_id = 
                                    (SELECT user_id FROM {self.__USERS_TABLE_NAME} WHERE username = '{username}') 
                                        AND products.device_id = '{device_id}'"""
            cursor.execute(query)
            all_data = cursor.fetchall()
            return self.__eventsToJson(all_data)

    def validateUser(self, credentials: str) -> str:
        payload_in = tuple(credentials.split(';')[:-1])
        user = payload_in[0]
        pass_ = payload_in[1]
        client_id = payload_in[2]

        cursor = self.__PGL_db_connection.cursor()
        query = f'SELECT COUNT(*) FROM {self.__USERS_TABLE_NAME} WHERE username = "{user}" AND password = "{pass_}"'

        try:
            cursor.execute(query)
            rows = cursor.fetchone()
            count = rows[0]
            cursor.reset()
            cursor.close()
            if (count > 0):
                return 'VALID', client_id
            else:
                return 'INVALID', client_id

        except mysql.Error as err:
            Warning.warn("Failed to validate user")
            return 'INVALID', client_id

# endregion
