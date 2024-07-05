import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import logging
import os

# MySQL connection details
user = 'digitoon'
password = 'digitoon123'
host = 'localhost:3306'
database = 'logs'
table_name = 'xlogs'

# logging config
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(filename=os.path.join('./logs', 'pyplot.log'), filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Create an SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# Query to retrieve data
query = f"SELECT * FROM {table_name}"

# Fetch data into a pandas DataFrame
try:
    df = pd.read_sql(query, engine)
    logging.info("Data retrieved successfully!")
    
    # Display the first few rows of the DataFrame
    logging.info(df.head())

except Exception as e:
    logging.error(f"Error: {e}")


# Close the connection
engine.dispose()

#/////////////////////////////

# Responsed Time Distribution
daily_counts = df[df['StatusCode'] == 200].groupby(df['Timestamp'].dt.date)['StatusCode'].count()

# Create the time series chart
plt.figure(figsize=(10, 5))
plt.plot(daily_counts.index, daily_counts.values, marker='o', linestyle='-', color='b')
plt.title('Responsed Time Distribution')
plt.xlabel('Date 20224')
plt.ylabel('Status_Code 200 Count')
plt.grid(True)
plt.show()

#///////////////////////////////////////////////

# Error Rate Over Time
daily_error_rate = df.groupby(df['Timestamp'].dt.date)['StatusCode'].apply(lambda x: (x != 200).sum() / len(x))

# Create the time series chart
plt.figure(figsize=(10, 5))
plt.plot(daily_error_rate.index, daily_error_rate.values, marker='o', linestyle='-', color='r')
plt.title('Error Rate Over Time')
plt.xlabel('Date 2024')
plt.ylabel('Error Rate')
plt.grid(True)
plt.show()

#////////////////////////////////////////

# Status_Code Counts
status_code_counts = df['StatusCode'].value_counts().sort_values(ascending=True)

# Create a bar gauge chart
plt.barh(status_code_counts.index.astype(str), status_code_counts.values)
plt.xlabel('Count')
plt.ylabel('Status_Code')
plt.title('Status_Code Counts')
plt.show()

#/////////////////////////////////

# Top 15 Errored URLs
url_errors = df[df['StatusCode'] != 200].groupby('URL').size().sort_values(ascending=True).tail(15)

# Create a bar gauge chart
plt.barh(url_errors.index, url_errors.values, color='purple')
plt.xlabel('Error Count')
plt.ylabel('URL')
plt.title('Top 15 Errored URLs')
plt.show()

#///////////////////////////////


# Precentage Of Each Accessed URLs
url_hits = df[df['StatusCode'] != 200].groupby(df['URL'].str.split('?').str[0]).size().sort_values(ascending=False)

# Create a pie chart
plt.figure(figsize=(8, 8))
plt.pie(url_hits, labels=url_hits.index, autopct='%1.1f%%', colors=plt.cm.Paired.colors)
plt.title('Precentage Of Each Accessed URLs (Excluding Query Parameters)')
plt.show()

#///////////////////////////////

# Hourly Request Counts
df['hour'] = df['Timestamp'].dt.hour
hourly_counts = df.groupby('hour')['hour'].count()

# Create a bar chart
plt.bar(hourly_counts.index, hourly_counts.values, color='purple')
plt.xlabel('Hour')
plt.ylabel('Request Count')
plt.title('Hourly Request Counts')
plt.xticks(range(24))
plt.show()

#////////////////////////////////

# Traffic Volume Over Time
traffic_vol = df[df['ResponseSize'].notnull()].groupby('Timestamp')['ResponseSize'].sum()

# Create a time series chart
plt.figure(figsize=(10, 6))
plt.plot(traffic_vol.index, traffic_vol.values, marker='o', color='purple')
plt.xlabel('Time')
plt.ylabel('Total Traffic (Bytes)')
plt.title('Traffic Volume Over Time')
plt.grid(True)
plt.show()

#///////////////////////////////

# Average Response Size by Hour
df['hour'] = df['Timestamp'].dt.hour
hourly_avg_response = df.groupby('hour')['ResponseSize'].mean()

# Create a bar chart
plt.bar(hourly_avg_response.index, hourly_avg_response.values, color='purple')
plt.xlabel('Hour')
plt.ylabel('Average Response Size (Bytes)')
plt.title('Average Response Size by Hour')
plt.xticks(range(24))
plt.show()

#/////////////////////////////////

# Top 10 URLs by Hits
top_urls = df.groupby('URL')['URL'].count().sort_values(ascending=True).tail(10)

# Create a bar gauge chart
plt.barh(top_urls.index, top_urls.values, color='red')
plt.xlabel('Hits')
plt.ylabel('URL')
plt.title('Top 10 URLs by Hits')
plt.show()

#////////////////////////////

# Top 10 Query Params Hit
# Expand the QueryParams column into its own DataFrame as series
params_df = df['QueryParams'].apply(pd.Series)

# Reshape the DataFrame to have one row per parameter-value pair
reshaped_df = params_df.melt(var_name='Parameter', value_name='Value').dropna()

# Count the occurrences of each parameter-value pair
counts = reshaped_df.groupby(['Parameter', 'Value']).size()

# Get the top 10 parameter-value pairs
top_10 = counts.nlargest(10)

# Create a bar gauge chart
top_10.plot(kind='barh', figsize=(10, 6))
plt.xlabel('Hits')
plt.title('Top 10 Query Parameters With Their Values')
plt.gca().invert_yaxis()  # Invert the y-axis to have the highest count at the top
plt.show()