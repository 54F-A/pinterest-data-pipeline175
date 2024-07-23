from multiprocessing import Process
from sqlalchemy import text
from time import sleep
import boto3
import json
import random
import requests
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


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    """Runs infinite loop that selects random rows from multiple database tables and prints them.
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')