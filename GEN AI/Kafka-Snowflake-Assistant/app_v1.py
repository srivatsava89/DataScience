from langchain_groq import ChatGroq
from langchain_community.chat_models import ChatSnowflakeCortex
import os
from app_pass import *

# Initialize Groq model
groq_model = ChatGroq(
    model="mixtral-8x7b-32768",
    api_key=os.getenv("GROQ_API_KEY"),
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

# Initialize Snowflake model
snowflake_model = ChatSnowflakeCortex()

def query_snowflake(question):
    # Create messages for Groq model
    messages = [
        {"role": "system", "content": "You are a helpful assistant that translates user questions into SQL queries."},
        {"role": "user", "content": question}
    ]
    
    # Get SQL query from Groq model
    sql_query_response = groq_model.invoke(messages)
    
    # Execute SQL query on Snowflake
    snowflake_response = snowflake_model.invoke([{"role": "user", "content": sql_query_response}])
    
    return snowflake_response

# Example usage
if __name__ == "__main__":
    api_key=os.getenv("GROQ_API_KEY")
    user_question = input("Ask a question about your data: ")
    result = query_snowflake(user_question)
    print(result)