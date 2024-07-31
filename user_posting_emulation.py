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



def post_to_api(data):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    invoke_url = "https://6oqdd8f045.execute-api.us-east-1.amazonaws.com/test"

    if "pin" in data:
        payload = json.dumps({
            "records": [{     
                "value": {
                            "index": data["pin"]["index"], 
                            "unique_id": data["pin"]["unique_id"],
                            "title": data["pin"]["title"],
                            "description": data["pin"]["description"],
                            "poster_name": data["pin"]["poster_name"],
                            "follower_count": data["pin"]["follower_count"],
                            "tag_list": data["pin"]["tag_list"],
                            "is_image_or_video": data["pin"]["is_image_or_video"],
                            "image_src": data["pin"]["image_src"],
                            "downloaded": data["pin"]["downloaded"],
                            "save_location": data["pin"]["save_location"],
                            "category": data["pin"]["category"]
                }
            }]
        })
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        print(response.status_code)
    
    if "geo" in data:
        payload = json.dumps({
            "records": [{     
                "value": {
                            "ind": data["geo"]["ind"], 
                            "timestamp": data["geo"]["timestamp"].strftime("%Y-%m-%d %H:%M:%S") ,
                            "latitude": data["geo"]["latitude"],
                            "longitude": data["geo"]["longitude"],
                            "country": data["geo"]["country"]
                }
            }]
        })
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        print(response.status_code)
    
    if "user" in data:
        payload = json.dumps({
            "records": [{     
                "value": {
                            "ind": data["user"]["ind"], 
                            "first_name": data["user"]["first_name"],
                            "last_name": data["user"]["last_name"],
                            "age": data["user"]["age"],
                            "date_joined": data["user"]["date_joined"].strftime("%Y-%m-%d %H:%M:%S")
                }
            }]
        })
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        print(response.status_code)

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
            
            data = {"pin": pin_result, "geo": geo_result, "user": user_result}
            print(data)
            post_to_api(data)

if __name__ == "__main__":
    run_infinite_post_data_loop()