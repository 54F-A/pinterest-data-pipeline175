# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib


# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-12aa97d84d77-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-12aa97d84d77-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location1 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.pin/partition=0/*.json" 
file_location2 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.geo/partition=0/*.json" 
file_location3 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"

# Read in JSONs from mounted S3 bucket
df1 = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location1)

df2 = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location2)

df3 = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location3)

# Display pin DataFrame
displayHTML("<h2>df_pin:</h2>")
display(df1)

# Display geo DataFrame
displayHTML("<h2>df_geo:</h2>")
display(df2)

# Display user DataFrame
displayHTML("<h2>df_user:</h2>")
display(df3)