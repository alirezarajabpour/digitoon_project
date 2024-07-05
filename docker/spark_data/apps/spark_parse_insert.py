from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import re
import json

# Init
def init_spark():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:8.0.26 pyspark-shell'
    sp = SparkSession.builder \
        .master('spark://spark-master:7077') \
        .appName('Submit-App-2') \
        .config('spark.driver.memory', '512M')  \
        .config('spark.executor.memory', '512M')  \
        .config('spark.executor.instances', '1')  \
        .config('spark.executor.cores', '1')  \
        .config("spark.cores.max", "1") \
        .getOrCreate()
    sc = sp.sparkContext
    return sp,sc
spark,sc = init_spark()

# Parse log UDF
def parse_log_line(log_line):
    # Extract regex
    pattern = r'(\d+\.\d+\.\d+\.\d+|NULL) - - \[(.*?)\] "(.*?)" (\d+) (\d+)?'
    match = re.match(pattern, log_line)
    if match:
        ip_address, timestamp, request, status_code, response_size = match.groups()
        # Extract query parameters separately
        query_params = re.findall(r'\?(.*?) HTTP', request)

        # Format the timestamp
        timestamp = timestamp.replace(' +0000', '')

        # Create a dictionary for query parameters
        params_dict = {}
        for param in query_params[0].split('&'):
            key, value = param.split('=')
            params_dict[key] = value

        # Set None for missing or invalid fields
        ip_address = None if ip_address == "NULL" else ip_address
        response_size = int(response_size) if response_size else None

        return Row(
            IPAddress=ip_address,
            Timestamp=timestamp,
            RequestMethod=request.split()[0],
            URL=request.split()[1],
            StatusCode=int(status_code),
            ResponseSize=response_size,
            QueryParams=json.dumps(params_dict)   # Store as JSON
        )
    else:
        print(f"Invalid log line: {log_line}")
        return Row()

# Schema
schema = StructType([
    StructField("IPAddress", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("RequestMethod", StringType(), True),
    StructField("URL", StringType(), True),
    StructField("StatusCode", IntegerType(), True),
    StructField("ResponseSize", IntegerType(), True),
    StructField("QueryParams", StringType(), True)
])

# Define udf
parse_log_line_udf = udf(parse_log_line, schema)

def main():
  global spark,sc   
  url = "jdbc:mysql://mysql:3306/logs"
  properties = {
    "user": "digitoon",
    "password": "digitoon123",
    "driver": "com.mysql.jdbc.Driver"
  }

  # Read file
  directory = "/opt/bitnami/spark/data"
  df = spark.read.text(directory + "/*.txt")

  # Apply udf
  df = df.withColumn("ParsedData", parse_log_line_udf(col("value")))
  df = df.select("ParsedData.*")
  
  # Change the Timestamp column type 
  df = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MMM/yyyy:HH:mm:ss"))
  
  # Remove duplicates
  df = df.dropDuplicates()

  print("Number of rows:", df.count())
  df.show(5)
  print("-="*30)
  
  # Insert into table
  if df.count() > 0:
        df.write.jdbc(url=url, table="xlogs", mode='append', properties=properties)
        print("Data written to the database.")
  else:
        print("No valid data to write to the database.")

if __name__ == '__main__':
  main()
