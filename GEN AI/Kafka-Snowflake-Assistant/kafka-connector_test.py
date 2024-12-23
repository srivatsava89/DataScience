# streamlit_app.py
import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from snowflake.snowpark import Session
import json
import os
import time
import google.generativeai as genai
from app_secrets import *
import snowflake.connector

# Configure Generative AI
genai.configure(api_key='AIzaSyC-jVXgj0ZNbElTyJ1w66gsi5Bgh_pU3mo')

# Set up the model
generation_config = {
    "temperature": 0.4,
    "top_p": 1,
    "top_k": 32,
    "max_output_tokens": 4096,
}

safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"}
]

model = genai.GenerativeModel(model_name="gemini-pro",
                              generation_config=generation_config,
                              safety_settings=safety_settings)

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
            try:
                session.write_pandas(df, table_name,auto_create_table=True)  # Insert valid records into Snowflake table
                st.success(f"Inserted record(s) into {table_name}: {record}")
            except Exception as e:
                st.error(f"Failed to insert record: {e}")
                log_invalid_data(record, ["Insertion failed"])
        else:
            log_invalid_data(record, errors)
            st.warning(f"Invalid record logged: {record}, Errors: {errors}")

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
# Function to fetch data from Snowflake based on generated SQL query
def fetch_data_from_snowflake(query):
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        print("*"*100,query)
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        results = [dict(zip(columns, row)) for row in rows]
        
        return results  # Return results with headers
    finally:
        cursor.close()
        conn.close()

def generate_gemini_response(question):
    """Generate SQL query using Generative AI and fetch data from Snowflake."""
    prompt_parts = [
    """You are an expert in converting English questions to SQL code for SNOWFLAKE Database! You are expert in writing snowflake queries! . get the details from snowflake connection like database, tablename and schema and form the query.you are smart enough to ask questions if you dont understand.
    \n```\n\nDont include ``` and \n or \n in the output and also dont include ` and '\n' and 'sql' in output
    here databasename referes to snowflake database
    schemaname refers to snowflake schema name
    tablename referes to snowflake table name 
    example:
    use asks : what is the count of the table 
    result : select count(*) from  HACKATHON.KAFKA_SNOWFLAKE.tablename;
    example:
    user : what are the tables 
    result : SELECT * FROM  HACKATHON.KAFKA_SNOWFLAKE.tablename
    exmaple :
    user : get list of columns for call_center and catalog_page
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME in ('CALL_CENTER','CATALOG_PAGE') order by table_name
    """,question
]
    
    response = model.generate_content(prompt_parts)
    
    # Fetch data from Snowflake using the SQL generated by Gemini
    output = fetch_data_from_snowflake(response.text.replace('\n', ' '))
    
    return output

# Streamlit UI Components
st.title("Generative AI Snowflake Assistant")

# Left Sidebar for Kafka Consumption
with st.sidebar:
    st.header("Kafka Data Ingestion")
    
    topic_name = st.text_input("Enter Kafka Topic Name:")
    table_name = st.text_input("Enter Table Name:", value="my_table")

    if st.button("Start Consuming"):
        if topic_name and table_name:
            consume_data(topic_name, table_name)
            st.success(f"Started consuming from topic '{topic_name}'")
        else:
            st.error("Please enter valid topic name and table name.")

# Main area for Generative AI Assistant
st.header("Ask the Snowflake Assistant")
user_query = st.text_input("What would you like to know about your data?")

if st.button("Get Insights"):
    if user_query:
        response = generate_gemini_response(user_query)
        
        # Display the results with headers
        if isinstance(response, list) and len(response) > 0:
            df = pd.DataFrame(response)  # Convert result to DataFrame for better display
            st.write(df)
            
            # Visualization based on the result DataFrame
            if len(df.columns) >= 2:  # Ensure there are at least two columns for visualization
                chart_type = st.selectbox("Select Chart Type", ["Bar Chart", "Line Chart", "Area Chart"])
                
                if chart_type == "Bar Chart":
                    st.bar_chart(df.set_index(df.columns[0]))  # Use first column as index for bar chart
                elif chart_type == "Line Chart":
                    st.line_chart(df.set_index(df.columns[0]))  # Use first column as index for line chart
                elif chart_type == "Area Chart":
                    st.area_chart(df.set_index(df.columns[0]))  # Use first column as index for area chart
                
            else:
                st.write("Not enough data to create a visualization.")
        else:
            st.write("No results found.")
