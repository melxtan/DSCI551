# %%

import os
import re
import pandas as pd 
import sqlparse
import pymysql
import pymongo
import psycopg2
from pymongo import MongoClient
from psycopg2.extras import RealDictCursor
from openai import AzureOpenAI
from bson import ObjectId

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
def connect_to_rdbms():
    """Establishes a connection to an RDBMS."""
    return pymysql.connect(host="18.224.56.248", user="root", password="Dsci-551", database="dsci551")

#连接 MongoDB
def connect_to_nosql():
    """Establishes a connection to a NoSQL database."""
    try:
        client = MongoClient("mongodb://18.224.56.248:27017/")
        # Test the connection
        client.server_info()
        db = client["world"]  # Use the existing database
        
        # Debug: Print available collections
        collections = db.list_collection_names()
        print("Available collections:", collections)
        
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        raise

def connect_to_postgres():
    """Establishes a connection to PostgreSQL database."""
    return psycopg2.connect(
        host="3.129.21.202",
        database="dvdrental",
        user="postgres",
        password="postgres",
        cursor_factory=RealDictCursor
    )

def get_sql_schema():
    """Retrieves MySQL database schema."""
    connection = connect_to_rdbms()
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
    connection = connect_to_postgres()
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
    db = connect_to_nosql()
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
    Ensures at least 3 involve table joins.
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

Your task is to generate {num_examples} realistic, clear, and diverse natural language questions that a non-technical user might ask about this data.

IMPORTANT:
- Ensure that **at least 3 of the questions involve JOINING two or more tables or collections**.
- Use natural and intuitive phrasing.
- Output the questions as a **numbered list only**, with no explanations or extra formatting.
    """

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "system", "content": system_prompt}],
            temperature=0.7,
            max_tokens=500,
        )
        content = response.choices[0].message.content
        questions = [line.split('. ', 1)[-1].strip() for line in content.split('\n') if line.strip()]
        
        # Ensure minimum number of questions is returned
        if len(questions) < num_examples:
            questions += ["(Placeholder for more example questions)"] * (num_examples - len(questions))

        return questions
    except Exception as e:
        print(f"Error generating example questions: {e}")
        return ["Example question generation failed."]


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


def generate_query(user_query: str, db_type: str) -> tuple:
    """Uses LLM to generate a database query based on schema."""
    if db_type == "mysql":
        schema = get_sql_schema()
        db_type_desc = "MySQL"
    elif db_type == "postgres":
        schema = get_postgres_schema()
        db_type_desc = "PostgreSQL"
    else:  # mongodb
        schema = get_nosql_schema()
        db_type_desc = "MongoDB"

    system_prompt = f"""
    You are a database query assistant. Based on the provided database schema, convert the following natural language query into a valid query.
    The target database type is {db_type_desc}.
    
    For {db_type_desc} queries:
    - Use the table names and column names exactly as provided in the schema
    - Follow {db_type_desc} syntax and conventions
    - For PostgreSQL, use proper parameterized queries with %s for parameters
    - For MySQL, use proper MySQL syntax
    
    For MongoDB queries:
    - Use the collection names exactly as provided in the schema
    - Output a valid MongoDB query in Python syntax
    - Start with db["collection_name"]
    
    Schema:
    {schema}
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_query}
    ]

    completion = call_llm_api(messages)
    extracted_query = extract_sql_from_response(completion)
    print("Generated query:", extracted_query)
    
    # Determine query type based on content
    if db_type in ["mysql", "postgres"]:
        if extracted_query.upper().startswith(("SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP")):
            return db_type.upper(), extracted_query
    else:  # mongodb
        if ".find(" in extracted_query or ".aggregate(" in extracted_query:
            return "NOSQL", extracted_query
    
    # Default to the specified database type
    return db_type.upper(), extracted_query

# %%

def validate_sql(sql_query: str) -> bool:
    """Validates the basic structure of an SQL statement."""
    try:
        statements = sqlparse.parse(sql_query)
        return bool(statements and len(statements) > 0)
    except Exception:
        return False


def execute_sql(sql_query: str):
    """Executes an SQL query with validation."""
    if not validate_sql(sql_query):
        return "SQL statement is invalid and cannot be executed."

    connection = connect_to_rdbms()
    try:
        with connection.cursor() as cursor:
            affected_rows = cursor.execute(sql_query)
            query_lower = sql_query.strip().lower()
            if query_lower.startswith(("select", "show", "describe")):
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                # Fetch results and convert to list of dictionaries
                results = []
                for row in cursor.fetchall():
                    result_dict = {}
                    for i, value in enumerate(row):
                        result_dict[columns[i]] = value
                    results.append(result_dict)
                return results
            else:
                connection.commit()
                return f"{affected_rows} rows affected."
    finally:
        connection.close()


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

def clean_mongodb_data(data):
    """Clean MongoDB data by converting special types to strings."""
    if isinstance(data, dict):
        return {k: clean_mongodb_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_mongodb_data(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, (pd.Timestamp, pd.DatetimeTZDtype)):
        return str(data)
    return data


def execute_nosql(nosql_query: str):
    """
    执行 MongoDB 查询。
    参数 nosql_query 预期为一个可以在 MongoDB 上执行的 Python 表达式字符串，
    例如："db['students'].find({'name': 'Alice'})"。

    注意：这里使用 eval 执行查询，请确保查询内容受信任。
    """
    print("Executing MongoDB query:", nosql_query)
    db = connect_to_nosql()  # Already returns the database object
    try:
        # 在安全上下文中执行查询，提供 db 变量供表达式使用
        result = eval(nosql_query, {"db": db, "ObjectId": ObjectId})
        # 如果返回的是 pymongo Cursor，则转换为列表
        if hasattr(result, "sort") or hasattr(result, "batch_size"):
            result = list(result)
        print("MongoDB query result:", result)
        return result
    except Exception as e:
        error_msg = f"Error executing MongoDB query: {str(e)}"
        print(error_msg)
        return error_msg



