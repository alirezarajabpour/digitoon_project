import re
import pandas as pd
import json
from sqlalchemy import create_engine
import logging
import os

# logging config
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(filename=os.path.join('./logs', 'parse_insert.log'), filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# parse
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
        timestamp = pd.to_datetime(timestamp, format='%d/%b/%Y:%H:%M:%S')

        # Create a dictionary for query parameters
        params_dict = {}
        for param in query_params[0].split('&'):
            key, value = param.split('=')
            params_dict[key] = value

        # Set None for missing or invalid fields
        ip_address = None if ip_address == "NULL" else ip_address
        response_size = int(response_size) if response_size else None

        return {
            'IPAddress': ip_address,
            'Timestamp': timestamp,
            'RequestMethod': request.split()[0],
            'URL': request.split()[1],
            'StatusCode': int(status_code),
            'ResponseSize': response_size,
            'QueryParams': json.dumps(params_dict)  # Store as JSON
        }
    else:
        logging.warning(f"Invalid log line: {log_line}")
        return None
    
# Read log data from the text file
log_file_path = 'nginx_logs.txt'
with open(log_file_path, 'r') as log_file:
    log_lines = log_file.readlines()

# Parse each log line and create a DataFrame
parsed_data_list = [parse_log_line(line.strip()) for line in log_lines if line.strip()]
df = pd.DataFrame(parsed_data_list)

# Remove duplicates
df_cleaned = df.drop_duplicates()

# MySQL connection details
user = 'digitoon'
password = 'digitoon123'
host = 'localhost:3306'
database = 'logs'
table_name = 'xlogs'

# Create an SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# Insert cleaned data into MySQL
try:
    df_cleaned.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    logging.info("Data inserted successfully!")
    print("Data inserted successfully!")
except Exception as e:
    logging.error(f"Failed to insert data: {e}")
    print("Failed to insert data!")

# Close the connection
engine.dispose()
