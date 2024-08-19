from db_connector import AWSDBConnector
from sqlalchemy import text
from time import sleep
import json
import random
import requests


new_connector = AWSDBConnector()


def post_to_api(data):
    """
    Posts data to an API endpoint.

    Args:
        data (dict): A dictionary containing the data to be posted.
    """
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    base_invoke_url = "https://6oqdd8f045.execute-api.us-east-1.amazonaws.com/test"

    if "pin" in data:
        invoke_url = base_invoke_url + "/topics/12aa97d84d77.pin"
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
        invoke_url = base_invoke_url + "/topics/12aa97d84d77.geo"
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
        invoke_url = base_invoke_url + "/topics/12aa97d84d77.user"
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
            post_to_api(data)

if __name__ == "__main__":
    run_infinite_post_data_loop()