# streamlit_app.py
import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from snowflake.snowpark import Session
import json
import os
from app_secrets import *
import time
from snowflake.connector.pandas_tools import write_pandas


# Snowflake connection parameters
snowflake_config = {
    'account': SF_ACCOUNT,
    'user': SF_USER,
    'password': SF_PASSWORD,
    'database': SF_DATABASE,
    'schema': SF_SCHEMA,
    'warehouse': SF_WAREHOUSE
}

# Create a Snowpark session
session = Session.builder.configs(snowflake_config).create()

def validate_data(record):
    """Validate incoming data."""
    errors = []
    
    if not isinstance(record.get('VOLUME'), int):
        errors.append("volume must be an integer")
    
    if not isinstance(record.get('OPENING_PRICE'), (float, int)):
        errors.append("opening_price must be a float or int")
    
    if not isinstance(record.get('MARKET_CAP'), (float, int)):
        errors.append("market_cap must be a float or int")
        
    return errors

def log_invalid_data(record, errors):
    """Log invalid records to a CSV file."""
    log_path = 'invalid_records.csv'
    
    # Create DataFrame for invalid record
    df = pd.DataFrame([record])
    df['errors'] = [', '.join(errors)]
    
    # Append to CSV file
    if not os.path.isfile(log_path):
        df.to_csv(log_path, index=False)
    else:
        df.to_csv(log_path, mode='a', header=False, index=False)

def consume_data(topic_name, table_name):
    """Consume data from Kafka and insert into Snowflake."""
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'data_consumer_group2',
        'auto.offset.reset': 'latest'
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])
    
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second
        print(msg)
        if msg is None:
            time.sleep(10)
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            st.error(f"Consumer error: {msg.error()}")
            break
        
        # Process the incoming message
        record = json.loads(msg.value().decode('utf-8'))
        
        errors = validate_data(record)
        
        if not errors:
            # Insert valid records into Snowflake table
            df = pd.DataFrame([record])
            print(df)
            #df.write_pandas(session.table(table_name), auto_create_table=True)
            #df.describe().write.mode("overwrite").save_as_table(table_name)
            try:
                #success, num_chunks, num_rows, _ = write_pandas(session, df, table_name)
                session.write_pandas(df, table_name, auto_create_table=True)
                st.success(f"Inserted  record(s) into {table_name} and records are : {record}")
            except Exception as e:
                st.error(f"Failed to insert record: {e}")
                log_invalid_data(record, ["Insertion failed"])
        else:
            log_invalid_data(record, errors)
            st.warning(f"Invalid record logged: {record}, Errors: {errors}")
            st.success(f"Inserted record: {record}")
        

# Streamlit UI Components
st.title("Kafka to Snowflake Data Ingestion")

topic_name = st.text_input("Enter Kafka Topic Name:")
table_name = st.text_input("Enter Table Name:", value="my_table")

if st.button("Start Consuming"):
    if topic_name and table_name:
        consume_data(topic_name, table_name)
        st.success(f"Started consuming from topic '{topic_name}'")
    else:
        st.error("Please enter valid topic name and table name.")
