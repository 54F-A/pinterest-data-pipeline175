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

This project involves the creation of a robust data pipeline designed for extracting and processing data from an AWS-hosted database, specifically focusing on Pinterest data. The pipeline is engineered to manage large datasets, construct efficient data pipelines, and integrate with real-time data processing tools using AWS and Kafka.

By leveraging Spark for data processing and AWS S3 for storage, the project ensures scalability and real-time data handling capabilities. The integration of Kafka for data transfer further enhances the pipeline's efficiency, making it a valuable tool for understanding and analysing large-scale data.

__AWS Resource Configuration__:

- S3 Bucket Setup

- Custom Plugin Development & MSK Connectors

__Data Extraction and Processing__:

- Delta Table Reading

- Mounting S3 Bucket

__Data Loading__:

- Loading JSON Data

__Data Cleaning and Transformation__:

- Coordinate Transformation

- Column Management

- Handling Missing and Inconsistent Data

- Numeric Conversion

- String Clean-up

- Column Reordering

- Name Concatenation

- Data Type Conversion

__Visualisation and Verification__:

- Data Display

- Data Schema Display

---

## Installation & Usage Instructions

To run the project, you need to have the required database credentials in a YAML file and a .pem file with your RSA PRIVATE KEY.

Follow these steps:

- Clone the repository to your local machine: __`git clone https://github.com/54F-A/pinterest-data-pipeline175.git`__

- Navigate to the project directory: __`cd pinterest-data-pipeline/`__

- Install the required packages: __`pip install -r requirements.txt`__

- Ensure your db_creds.yaml file is correctly set up with your database credentials.

- Grant permissions to the .pem file: __`chmod 400 /path/to/private_key.pem`__

- Open a WSL (ubuntu) terminal & connect to the EC2 instance: __`ssh -i "<key_pair.pem>" <Public DNS>`__

- Ensure that the client.properties & kafka-rest.properties are configured.

- Navigte to the bin folder of the Confluent platform: __`cd confluent-7.2.0/bin/`__

- Start the REST proxy: __`./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`__

- On a new terminal, run the data posting file: __`python user_posting_emulation.py`__

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