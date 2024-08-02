# Pinterest Data Pipeline

### Table of Contents:

#### 1. [Overview: The Project Description](#1-overview-the-project-description)
#### 2. [Installation & Usage Instructions](#2-installation--usage-instructions)
#### 3. [File Structure of the Project](#3-file-structure-of-the-project)
- #### [AWSDBConnector](#awsdbconnector)
- #### [Data Transfer to Kafka Topics](#data-transfer-to-kafka-topics)
- #### [Post Data to the API](#post-data-to-the-api)
#### 4. [License Information](#4-license-information)

---

## Overview: The Project Description

This project is focused on creating a data pipeline for extracting and processing data from an AWS-hosted database, specifically dealing with Pinterest data. This project helps to understand handling large datasets, data pipeline construction, database interaction, and real-time data processing using AWS and Kafka.

AWS resources were configured to support the pipeline's data flow; such as setting up an S3 bucket, creating a custom plugin & configuring MSK Connectors, to allow transfer of data between Kafka topics & an Amazon S3 bucket. These steps ensured that the data pipeline operates smoothly and integrates with AWS services for data storage and processing. The setup extracts random rows of data from an AWS-hosted database and sends them to Kafka topics via an API.

---

## Installation & Usage Instructions

To run the project, you need to have the required database credentials in a YAML file and a .pem file with your RSA PRIVATE KEY.

Follow these steps:

- Clone the repository to your local machine: __`git clone https://github.com/54F-A/pinterest-data-pipeline175.git`__

- Navigate to the project directory: __`cd pinterest-data-pipeline/`__

- Install the required packages: __`pip install -r requirements.txt`__

- Ensure your db_creds.yaml file is correctly set up with your database credentials.

- Run the data extraction script: __`python main.py`__

---

## File Structure of the Project

### AWSDBConnector

A class to handle AWS database connections.

__Attributes__:

- config (dict): Configuration parameters loaded from the credentials file.

- HOST (str): Database host address.

- USER (str): Database username.

- PASSWORD (str): Database password.

- DATABASE (str): Database name.

- PORT (int): Database port.

__Method__:

- `__init__(self, creds_file='db_creds.yaml')`: Initialises the AWSDBConnector instance.

- `load_config(self, creds_file)`: Loads database configuration from a YAML file.

- `create_db_connector(self)`: Creates a SQLAlchemy engine for connecting to the database.

---

### Data Transfer to Kafka Topics:

Extracts data from the following tables:

- pinterest_data

- geolocation_data

- user_data

__Method__:

- `Pin`: If the data contains a "pin" key, it constructs a payload with the pin data and sends it to the pin Kafka topic.

- `Geo`: If the data contains a "geo" key, it constructs a payload with the geo data and sends it to the geo Kafka topic.

- `User`: If the data contains a "user" key, it constructs a payload with the user data and sends it to the user Kafka topic.

- `post_to_api(data)`: Sends data to specific Kafka topics via an API endpoint.

__API Request__:

Sends a POST request to the appropriate Kafka topic endpoint with the constructed payload.

---

### Post Data to the API:

Selects random rows from database tables and posts them to the API.

__Method__:

- `sleep(random.randrange(0, 2))`: Continuously runs, pausing for a random duration between 0 and 2 seconds in each iteration.

- `random_row = random.randint(0, 11000)`: Chooses a random row index between 0 and 11,000.

- `engine = new_connector.create_db_connector()`: Establishes a connection to the database.

- `text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")`: Executes a SQL query to select a row from the pinterest_data table

- `pin_result = dict(row._mapping)`: Converts the result to a dictionary.

- `text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")`: Executes a SQL query to select a row from the geolocation_data table.

- `geo_result = dict(row._mapping)`: Converts the result to a dictionary.

- `user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")`: Executes a SQL query to select a row from the user_data table.

- `user_result = dict(row._mapping)`: Converts the result to a dictionary.

- `data = {"pin": pin_result, "geo": geo_result, "user": user_result}`: Combines the extracted data into a dictionary with keys "pin", "geo", and "user".

- `post_to_api(data)`: Sends the data to the API using the post_to_api(data) function.

- `run_infinite_post_data_loop()`: Runs an infinite loop to select random rows from multiple database tables and posts them to the API.

---

## License Information

This project is licensed under the MIT License.