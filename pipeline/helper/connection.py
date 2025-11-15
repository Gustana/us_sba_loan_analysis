from dotenv import load_dotenv
import psycopg2

class Connection:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, user: str, password: str, host: str, db_name: str, port: int):
        """
        Params:
            user (str): user
            pass (str): password
            host (str): host
            database (str): database name
            port (int): database port
        """

        if not self._initialized:
            self.connection = psycopg2.connect(
                user = user,
                password = password,
                host = host,
                database = db_name,
                port = port
            )

            self.cursor = self.connection.cursor()

            self._initialized = True
    
    def close_connection(self):
        """
        Close connection
        """

        self.cursor.close()
        self.connection.close()