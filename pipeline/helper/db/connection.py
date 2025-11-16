import os

from dotenv import load_dotenv
import psycopg2

class Connection:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Params:
            user (str): user
            pass (str): password
            host (str): host
            database (str): database name
            port (int): database port
        """

        load_dotenv()

        if not self._initialized:
            self.connection = psycopg2.connect(
                user = os.getenv("DB_USER"),
                password = os.getenv("DB_PASS"),
                host = os.getenv("DB_HOST"),
                database = os.getenv("DB_NAME"),
                port = os.getenv("DB_PORT")
            )

            self.cursor = self.connection.cursor()

            self._initialized = True
    
    def close_connection(self):
        """
        Close connection
        """

        self.cursor.close()
        self.connection.close()