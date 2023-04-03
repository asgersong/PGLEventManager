import mysql.connector as mysql
import json

class PGLEventManagerModel:
    """Model to store timestamp events in mysql database.
    The model handles all interaction with the database. """

    USERS_TABLE_NAME = "users"
    JOURNEY_TABLE_NAME = "journey"
    PRODUCT_TABLE_NAME = "products"

    USERS_TABLE_DESCRIPTION: str = """users 
                                       (username VARCHAR(320) NOT NULL,
                                        password VARCHAR(255) NOT NULL, 
                                        usertype VARCHAR(30) NOT NULL, 
                                        PRIMARY KEY(username) )"""

    JOURNEY_TABLE_DESCRIPTION: str = """ journey 
                                        (journey_id int NOT NULL AUTO_INCREMENT, 
                                        datetime VARCHAR(30) NOT NULL, 
                                        timestamp VARCHAR(30) NOT NULL, 
                                        device int NOT NULL,
                                        PRIMARY KEY (journey_id) )"""

    PRODUCTS_TABLE_DESCRIPTION: str = """products 
                                    (product_id int NOT NULL AUTO_INCREMENT,
                                    device  int NOT NULL,
                                    user    VARCHAR(320) NOT NULL,
                                    PRIMARY KEY (product_id),
                                    FOREIGN KEY (user) REFERENCES users(username))"""

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
                self.createDatabase()
                print(f"Database {self.__database_name} created successfully.")
            else:
                print(f'Failed connecting to database with error: {err}')

    # disconnect from the database
    def disconnectDB(self) -> None:
        self.__PGL_db_connection.disconnect()
        print("Disconnected from database")

    # creates database with parameters from __init__
    def createDatabase(self) -> None:
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
            # create events table with two columns
            # create users table with two columns
            cursor.execute(f"CREATE TABLE {self.USERS_TABLE_DESCRIPTION}")
            cursor.execute(f'CREATE TABLE {self.PRODUCTS_TABLE_DESCRIPTION}')
            cursor.execute(f"CREATE TABLE {self.JOURNEY_TABLE_DESCRIPTION}")

        cursor.close()
        self.__PGL_db_connection = self.__database_name

    def userExists(self, username) -> bool:
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

    def store(self, event, table: str):
        # we should format the event here in respective columns and such
        try:
            # store timestamp event in 'events' table
            if table == self.JOURNEY_TABLE_NAME:
                cursor = self.__PGL_db_connection.cursor()
                query = f"INSERT INTO {self.JOURNEY_TABLE_NAME} (datetime, timestamp, device) VALUES (%s, %s, %s)"
                val = tuple(event.split(';')[:-1])
                cursor.execute(query, val)
                self.__PGL_db_connection.commit()
                print("Stored event in DB")

            # store user in 'users' table
            elif table == self.USERS_TABLE_NAME:
                cursor = self.__PGL_db_connection.cursor()

                # check that user doesn't already exist
                val = tuple(event.split(';')[:-1])
                # if no duplicates, insert in table
                if not self.userExists(val[0]):
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
            elif table == self.PRODUCT_TABLE_NAME:
                cursor = self.__PGL_db_connection.cursor()
                val = tuple(event.split(';')[:-1])
                user = val[0]
                if self.userExists(user):
                    query = f"INSERT INTO {self.PRODUCT_TABLE_NAME}(device, user) VALUES (%s, %s)"
                    cursor.execute(query, val)
                    self.__PGL_db_connection.commit()
                    print("Stored product in DB")
                else:
                    print("User not found. Didn't store products")

        except mysql.Error as err:
            print(f'Failed to insert into database with error: {err}')

        cursor.close()

    def getEvents(self, table: str, credentials: str) -> str:
        # returns all data related to the user from the database as string in json format
        if table == self.JOURNEY_TABLE_NAME:
            credentials = tuple(credentials.split(';')[:-1])  # get username
            username = credentials[0]
            # find all devices related to user from products table
            query = f'SELECT device FROM products WHERE user = "{username}"'
            cursor = self.__PGL_db_connection.cursor()
            journeys = []
            events = []
            cursor.execute(query)
            all_data = cursor.fetchall()  # get all devices related to user
            for row in all_data:
                # find all journeys related to device
                query = f'SELECT * FROM journey WHERE device = "{row[0]}"'
                cursor.execute(query)
                journeys = cursor.fetchall()
                for j in journeys:
                    events.append(j)

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
                return 'INVALID'
