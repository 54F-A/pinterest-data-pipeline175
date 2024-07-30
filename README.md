# Pinterest Data Pipeline

### Table of Contents:

#### 1. [Overview: The Project Description](#1-overview-the-project-description)
#### 2. [Installation & Usage Instructions](#2-installation--usage-instructions)
#### 3. [File Structure of the Project](#3-file-structure-of-the-project)
- #### [AWSDBConnector](#awsdbconnector)
- #### [Data Extraction](#data-extraction)
#### 4. [License Information](#4-license-information)

---

### Overview: The Project Description

This project is focused on creating a data pipeline for extracting and processing data from an AWS-hosted database, specifically dealing with Pinterest data. The pipeline selects random rows from multiple tables and prints them in an infinite loop. This project helps in understanding database connectivity, data extraction, and handling large datasets.

AWS resources were configured to support the pipeline's data flow; such as setting up an S3 bucket, creating a custom plugin & configuring MSK Connectors, to allow transfer of data between Kafka topics & an Amazon S3 bucket. These steps ensured that the data pipeline operates smoothly and integrates with AWS services for data storage and processing.

---

### Installation & Usage Instructions

To run the project, you need to have the required database credentials in a YAML file and a .pem file with your RSA PRIVATE KEY.

Follow these steps:

- Clone the repository to your local machine: __`git clone https://github.com/54F-A/pinterest-data-pipeline175.git`__

- Navigate to the project directory: __`cd pinterest-data-pipeline/`__

- Install the required packages: __`pip install -r requirements.txt`__

- Ensure your db_creds.yaml file is correctly set up with your database credentials.

- Run the data extraction script: __`python main.py`__

---

### File Structure of the Project

Connects to an AWS-hosted database using credentials from a YAML file.
Extracts random rows from multiple tables in an infinite loop.
Prints the extracted data to the console.

#### AWSDBConnector

A class to handle AWS database connections.

Attributes:

- config (dict): Configuration parameters loaded from the credentials file.

- HOST (str): Database host address.

- USER (str): Database username.

- PASSWORD (str): Database password.

- DATABASE (str): Database name.

- PORT (int): Database port.

Methods:

- `__init__(self, creds_file='db_creds.yaml')`: Initialises the AWSDBConnector instance.

- `load_config(self, creds_file)`: Loads database configuration from a YAML file.

- `create_db_connector(self)`: Creates a SQLAlchemy engine for connecting to the database.

#### Data Extraction

Extracts data from the following tables:

- pinterest_data

- geolocation_data

- user_data

The script runs an infinite loop to select and print random rows from these tables.

---

#### License Information

This project is licensed under the MIT License.