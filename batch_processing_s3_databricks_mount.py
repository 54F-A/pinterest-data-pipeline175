from pyspark.sql.functions import *
import urllib


delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

AWS_S3_BUCKET = "user-12aa97d84d77-bucket"
MOUNT_NAME = "/mnt/user-12aa97d84d77-bucket"
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

file_location1 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.pin/partition=0/*.json" 
file_location2 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.geo/partition=0/*.json" 
file_location3 = "/mnt/user-12aa97d84d77-bucket/topics/12aa97d84d77.user/partition=0/*.json" 
file_type = "json"
infer_schema = "true"

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