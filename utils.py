# %%

import os
import re
import pandas as pd 
import sqlparse
import pymysql
import pymongo
from openai import AzureOpenAI

# 从环境变量中获取配置参数（也可以在 constant.py 中统一管理）
endpoint = os.getenv("ENDPOINT_URL", "https://gerhut.openai.azure.com/")
deployment = os.getenv("DEPLOYMENT_NAME", "gpt-4o")
subscription_key = os.getenv("AZURE_OPENAI_API_KEY", "8f7f30756b2f44eaa303ea8f6e4b18fb")

# 初始化 Azure OpenAI 客户端
client = AzureOpenAI(
    azure_endpoint=endpoint,
    api_key=subscription_key,
    api_version="2024-05-01-preview",
)

def call_llm_api(messages: list) -> str:
    """Calls LLM API with the given messages."""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages
    )
    return response.choices[0].message.content

# 连接 SQL 数据库
def connect_sql():
    return pymysql.connect(
        host='18.224.56.248',
        user='root',
        password='Dsci-551',
        db='world',
        charset='utf8mb4'
    )

#连接 MongoDB
def connect_mongo():
    client = pymongo.MongoClient("mongodb://18.224.56.248:27017/", serverSelectionTimeoutMS=5000)
    return client["world"]

def get_sql_schema():
    """Retrieves MySQL database schema."""
    connection = connect_sql()
    schema = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                cursor.execute(f"DESCRIBE {table};")
                schema[table] = [column[0] for column in cursor.fetchall()]
    finally:
        connection.close()
    return schema


def get_postgres_schema():
    """Retrieves PostgreSQL database schema."""
    connection = connect_mongo()
    schema = {}
    try:
        with connection.cursor() as cursor:
            # Get all tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row['table_name'] for row in cursor.fetchall()]

            # Get columns for each table
            for table in tables:
                cursor.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table,))
                schema[table] = [row['column_name'] for row in cursor.fetchall()]
    finally:
        connection.close()
    return schema


def get_nosql_schema():
    """Retrieves MongoDB collection structure."""
    db = connect_mongo()
    schema = {}
    
    # Get all collections
    collections = db.list_collection_names()
    print("Available collections:", collections)
    
    # Get schema for each collection
    for collection in collections:
        # Get one document from the collection to determine fields
        document = db[collection].find_one()
        if document:
            # Remove _id field as it's MongoDB specific
            fields = [key for key in document.keys() if key != '_id']
            schema[collection] = fields
            print(f"Schema for {collection}:", fields)
    
    return schema


def generate_example_questions(schema: dict, db_type: str = "sql", num_examples: int = 5) -> list[str]:
    """
    Generate natural language example questions from a given database schema using Azure OpenAI.
    """
    table_descriptions = []
    for table, columns in schema.items():
        cols = ", ".join(columns)
        table_descriptions.append(f"- {table} ({cols})")

    schema_description = "\n".join(table_descriptions)

    system_prompt = f"""
You are a helpful assistant that helps users write natural language questions for querying a {db_type.upper()} database.

Here is the schema of the database:
{schema_description}

Generate {num_examples} realistic and useful example questions that a non-technical user might ask about this data.
Only output the questions as a numbered list.
    """

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt}
            ],
            temperature=0.7,
            max_tokens=300,
        )
        content = response.choices[0].message.content
        questions = [line.split('. ', 1)[-1].strip() for line in content.split('\n') if line.strip()]
        return questions
    except Exception as e:
        print(f"Error generating example questions: {e}")
        return ["Example question generation failed."]
# %%

def generate_query(user_query, db_type):
    """
    使用 LLM 解析自然语言，并结合数据库 Schema 生成 SQL/NoSQL 查询
    """
    schema = get_sql_schema() if db_type == "sql" else get_nosql_schema()

    system_prompt = f"""
    你是一个数据库查询助手，任务是将自然语言转换为数据库查询。
    当前数据库结构如下：
    {schema}
    请根据用户的查询生成正确的 {db_type.upper()} 语句。
    """

    messages = [
        {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
        {"role": "user", "content": [{"type": "text", "text": user_query}]}
    ]

    completion = client.chat.completions.create(
        model=deployment,
        messages=messages,
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        stream=False
    )

    return completion.choices[0].message.content


# %%

def extract_sql_from_response(llm_response: str) -> str:
    """
    Extracts SQL statements or MongoDB queries from an LLM response.
    If no code block is detected, returns the original response.
    """
    pattern = r"```[^\n]*\n([\s\S]*?)```"
    match = re.search(pattern, llm_response)
    if match:
        query = match.group(1).strip()
    else:
        query = llm_response.strip()

    # For MongoDB queries, extract only the db.collection part
    if "db[" in query or "db." in query:
        lines = query.split('\n')
        for line in lines:
            if "db[" in line or "db." in line:
                query = line.strip()
                if query.startswith("result = "):
                    query = query[9:]  # Remove "result = " prefix
                break

    return query

# %%

import sqlparse

def validate_sql(sql_query: str) -> bool:
    """Validates the basic structure of an SQL statement."""
    try:
        statements = sqlparse.parse(sql_query)
        return bool(statements and len(statements) > 0)
    except Exception:
        return False


def execute_sql(sql_query: str):
    """Execute SQL query and return result in a DataFrame."""

    # Ensure query is cleaned
    sql_query = extract_sql_from_response(sql_query)

    if not validate_sql(sql_query):
        return "SQL query is invalid and cannot be executed."

    connection = connect_sql()
    try:
        with connection.cursor() as cursor:
            affected_rows = cursor.execute(sql_query)
            query_lower = sql_query.strip().lower()
            if query_lower.startswith(("select", "show", "describe")):
                result = cursor.fetchall()
                if result:
                    # Fetch column names dynamically
                    column_names = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(result, columns=column_names)
                    return df
                else:
                    return "No results returned."
            else:
                connection.commit()
                return f"{affected_rows} rows affected."
    finally:
        connection.close()

# %%

def execute_postgres(query: str):
    """Executes a PostgreSQL query."""
    print("Executing PostgreSQL query:", query)
    connection = connect_to_postgres()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                print("Query results:", results)
                return results
            else:
                connection.commit()
                return f"{cursor.rowcount} rows affected."
    except Exception as e:
        print("Error executing PostgreSQL query:", str(e))
        raise
    finally:
        connection.close()


import re
import ast

def parse_mongo_query_string(query_str: str):
    """
    Parses a MongoDB query like:
    db.city.find({ "Name": "Paris" }, { "Population": 1 })
    Returns: collection_name, filter_dict, projection_dict
    """
    pattern = r"db\.([a-zA-Z0-9_]+)\.find\((\{.*?\})\s*,\s*(\{.*?\})\)"
    match = re.search(pattern, query_str, re.DOTALL)
    if not match:
        raise ValueError("Invalid MongoDB query format")

    collection_name = match.group(1)
    filter_dict = ast.literal_eval(match.group(2))
    projection_dict = ast.literal_eval(match.group(3))

    return collection_name, filter_dict, projection_dict


def execute_mongo(nosql_query: str):
    """
    Safely execute a MongoDB query string returned from the LLM.
    Converts result to DataFrame like SQL for consistent display.
    """
    db = connect_to_nosql()  # already gives you `db` object

    try:
        # Clean and extract actual MongoDB query string (if wrapped in code block)
        nosql_query = extract_query_from_response(nosql_query)  # like extract_sql_from_response

        # Parse query into components
        collection, filter_dict, projection_dict = parse_mongo_query_string(nosql_query)

        cursor = db[collection].find(filter_dict, projection_dict)
        result = list(cursor)

        if not result:
            return "No results returned."
        return pd.DataFrame(result)

    except Exception as e:
        return f"MongoDB 查询错误: {str(e)}"



