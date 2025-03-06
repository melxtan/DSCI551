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

# 连接 SQL 数据库
def connect_sql():
    return pymysql.connect(
        host='3.142.136.71',
        user='root',
        password='Dsci-551',
        db='world',
        charset='utf8mb4'
    )

#连接 MongoDB
# def connect_mongo():
#     client = pymongo.MongoClient("mongodb://localhost:27017/")
#     return client["management"]


# %%

def get_sql_schema():
    """从 SQL 数据库获取表结构"""
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

def get_nosql_schema():
    """从 MongoDB 获取 Collection 结构"""
    db = connect_mongo()
    schema = {}
    collections = db.list_collection_names()
    
    for collection in collections:
        document = db[collection].find_one()
        if document:
            schema[collection] = list(document.keys())
    return schema


# %%

def generate_query(user_query, db_type="sql"):
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
    从 LLM 回复中提取出 SQL 语句，去掉三重反引号。
    如果没有检测到代码块，则直接返回原始文本。
    """
    # 匹配三重反引号包裹的内容
    pattern = r"```[^\n]*\n([\s\S]*?)```"
    match = re.search(pattern, llm_response)
    if match:
        # 提取代码块内部的文本
        sql_query = match.group(1).strip()
    else:
        # 如果没有代码块，就直接返回原始回复
        sql_query = llm_response.strip()

    return sql_query


# %%

import sqlparse

def validate_sql(sql_query: str) -> bool:
    """验证 SQL 语句的基本合法性"""
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

def execute_mongo(mongo_query):
    """执行 MongoDB 查询"""
    db = connect_mongo()
    
    try:
        query_result = eval(f"db.{mongo_query}")  # 执行 MongoDB 语句
        return list(query_result) if isinstance(query_result, pymongo.cursor.Cursor) else query_result
    except Exception as e:
        return f"MongoDB 查询错误: {str(e)}"


# %%

# 测试 SQL 查询
# user_input = "学生姓名名单"
# sql_query = generate_query(user_input, db_type="sql")
# sql_query = extract_sql_from_response(sql_query)
# print("生成的 SQL 命令:")
# print(sql_query)

# if validate_sql(sql_query):
#     result = execute_sql(sql_query)
#     print("执行结果:", result)
# else:
#     print("SQL 语句无效！")

# 测试 MongoDB 查询
# mongo_query = generate_query(user_input, db_type="nosql")
# print("生成的 MongoDB 查询:")
# print(mongo_query)

# result = execute_mongo(mongo_query)
# print("查询结果:", result)



