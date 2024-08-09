from datetime import datetime
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
        """Initializes the AWSDBConnector instance.

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


def post_to_kinesis(data):
    """
    Posts data to API endpoints associated with Kinesis streams.

    Args:
        data (dict): A dictionary containing the data to be posted.
    """
    headers = {'Content-Type': 'application/json'}
    base_invoke_url = "https://6oqdd8f045.execute-api.us-east-1.amazonaws.com"
    
    if "pin" in data:
        invoke_url = base_invoke_url + "/test/streams/streaming-12aa97d84d77-pin/record"
        partition_key = 'partition-1'
        payload = {
            "StreamName": "streaming-12aa97d84d77-pin",
            "Data": json.dumps({
                "index": data["pin"].get("index", ""),
                "unique_id": data["pin"].get("unique_id", ""),
                "title": data["pin"].get("title", ""),
                "description": data["pin"].get("description", ""),
                "poster_name": data["pin"].get("poster_name", ""),
                "follower_count": data["pin"].get("follower_count", ""),
                "tag_list": data["pin"].get("tag_list", ""),
                "is_image_or_video": data["pin"].get("is_image_or_video", ""),
                "image_src": data["pin"].get("image_src", ""),
                "downloaded": data["pin"].get("downloaded", 0),
                "save_location": data["pin"].get("save_location", ""),
                "category": data["pin"].get("category", "")
            }),
            "PartitionKey": partition_key
        }
        print("Payload being sent to Kinesis:", json.dumps(payload, indent=4))
        response = requests.put(invoke_url, headers=headers, json=payload)
        print(response.status_code)
        print(response.text)
    
    if "geo" in data:
        invoke_url = base_invoke_url + "/test/streams/streaming-12aa97d84d77-geo/record"
        partition_key = 'partition-2'
        payload = {
            "StreamName": "streaming-12aa97d84d77-geo",
            "Data": json.dumps({
                "ind": data["geo"].get("ind", ""), 
                "timestamp": data["geo"].get("timestamp", "").isoformat() if data["geo"].get("timestamp", None) else None,
                "latitude": data["geo"].get("latitude", ""),
                "longitude": data["geo"].get("longitude", ""),
                "country": data["geo"].get("country", "")
            }),
            "PartitionKey": partition_key
        }
        print("Payload being sent to Kinesis:", json.dumps(payload, indent=4))
        response = requests.put(invoke_url, headers=headers, json=payload)
        print(response.status_code)
        print(response.text)

    if "user" in data:
        invoke_url = base_invoke_url + "/test/streams/streaming-12aa97d84d77-user/record"
        partition_key = 'partition-3'
        payload = {
            "StreamName": "streaming-12aa97d84d77-user",
            "Data": json.dumps({
                "ind": data["user"].get("ind", ""), 
                "first_name": data["user"].get("first_name", ""),
                "last_name": data["user"].get("last_name", ""),
                "age": data["user"].get("age", ""),
                "date_joined": data["user"].get("date_joined", "").isoformat() if data["user"].get("date_joined", None) else None
            }),
            "PartitionKey": partition_key
        }
        print("Payload being sent to Kinesis:", json.dumps(payload, indent=4))
        response = requests.put(invoke_url, headers=headers, json=payload)
        print(response.status_code)
        print(response.text)

def run_infinite_post_data_loop():
    """
    Continuously runs an infinite loop retrieving random rows from 3 database tables & posting the data to an API.

    Performs the following:
    1. Sleeps for a random duration between 0 and 2 seconds.
    2. Selects random row index from range 0 to 11,000.
    4. Retrieves a row of data from 3 tables.
    5. Converts the retrieved rows to dictionaries.
    6. Compiles the data into a dictionary with keys: 'pin', 'geo', and 'user'.
    8. Posts the compiled data to an API.
    """
    engine = new_connector.create_db_connector() 
    
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        data = {"pin": None, "geo": None, "user": None}

        with engine.connect() as connection:
            
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pin_result = None

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            geo_result = None

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            user_result = None
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            if pin_result:
                data = {"pin": pin_result, "geo": geo_result, "user": user_result}
                post_to_kinesis(data)
            else:
                print("No valid pin data found. Skipping post to Kinesis.")

if __name__ == "__main__":
    run_infinite_post_data_loop()
