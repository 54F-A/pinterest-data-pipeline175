import random
import sqlalchemy
import yaml


random.seed(100)


class AWSDBConnector:
    """A class to handle AWS database connections.

    Attributes:
        config (dict): Configuration parameters loaded from the credentials file.
        HOST (str): Database host address.
        USER (str): Database username.
        PASSWORD (str): Database password.
        DATABASE (str): Database name.
        PORT (int): Database port.
    """

    def __init__(self, creds_file='db_creds.yaml'):
        """Initialises the AWSDBConnector instance.

        Args:
            creds_file (str): The path to the database credentials YAML file.
        """
        self.config = self.load_config(creds_file)
        self.HOST = self.config['HOST']
        self.USER = self.config['USER']
        self.PASSWORD = self.config['PASSWORD']
        self.DATABASE = self.config['DATABASE']
        self.PORT = self.config['PORT']
    
    def load_config(self, creds_file):
        """Loads database configuration from a YAML file.

        Args:
            creds_file (str): The path to the database credentials YAML file.
        Returns:
            dict: The configuration parameters.
        """
        with open(creds_file, 'r') as file:
            config = yaml.safe_load(file)
        return config
        
    def create_db_connector(self):
        """Creates a SQLAlchemy engine for connecting to the database.

        Returns:
            sqlalchemy.engine.Engine: The SQLAlchemy engine for the database connection.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine