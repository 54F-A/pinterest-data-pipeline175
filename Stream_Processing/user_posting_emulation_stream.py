from db_connector import AWSDBConnector 
from sqlalchemy import text
from time import sleep
import json
import random
import requests


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

def extract_row_from_db(connection, table_name, row_index):
    """
    Extracts a single row from the specified table by row index.
    
    Args:
        connection: The database connection object.
        table_name (str): The name of the table to query.
        row_index (int): The index of the row to retrieve.

    Returns:
        dict: The retrieved row as a dictionary.
    """
    query_string = text(f"SELECT * FROM {table_name} LIMIT {row_index}, 1")
    selected_row = connection.execute(query_string)
    result = None
    
    for row in selected_row:
        result = dict(row._mapping)
    
    return result

def run_infinite_post_data_loop():
    """
    Continuously runs an infinite loop retrieving random rows from 3 database tables & posting the data to an API.
    
    Performs the following:
    1. Sleeps for a random duration between 0 and 2 seconds.
    2. Selects a random row index from the range 0 to 11,000.
    3. Retrieves a row of data from 3 tables.
    4. Converts the retrieved rows to dictionaries.
    5. Compiles the data into a dictionary with keys: 'pin', 'geo', and 'user'.
    6. Posts the compiled data to an API.
    """
    while True:
        sleep(random.uniform(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_result = extract_row_from_db(connection, 'pinterest_data', random_row)
            geo_result = extract_row_from_db(connection, 'geolocation_data', random_row)
            user_result = extract_row_from_db(connection, 'user_data', random_row)

            data = {"pin": pin_result, "geo": geo_result, "user": user_result}
            print(data)
            post_to_kinesis(data)

if __name__ == "__main__":
    run_infinite_post_data_loop()
