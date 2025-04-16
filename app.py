import streamlit as st
import pandas as pd
from utils import generate_query, execute_sql, execute_mongo, get_sql_schema, get_nosql_schema

def main():
    st.title("Natural Language to Database Query")

    # Choose database type
    db_choice = st.radio("Select Database Type", ("SQL", "NoSQL"))
    db_type = "sql" if db_choice == "SQL" else "nosql"

    # Fetch and display schema dynamically
    with st.expander("📚 View Database Schema"):
        if db_type == "sql":
            schema = get_sql_schema()
        else:
            schema = get_nosql_schema()

        for table, columns in schema.items():
            st.markdown(f"**{table}**")
            st.markdown(", ".join(f"`{col}`" for col in columns))

    # Example natural language questions
    with st.expander("💡 Example Questions You Can Ask"):
        st.markdown("""
        - What is the population of Tokyo?
        - List all official languages spoken in France.
        - Which countries are in Europe with a population over 50 million?
        - Show all cities in Japan with more than 1 million people.
        - What is the capital of Brazil?
        """)

    # User input
    user_query = st.text_area("Enter your query in natural language")

    if "generated_query" not in st.session_state:
        st.session_state.generated_query = None

    # Generate Query
    if st.button("Generate Query"):
        if user_query:
            st.session_state.generated_query = generate_query(user_query, db_type)

    # Display and execute generated query
    if st.session_state.generated_query:
        st.write("### Generated Query:")
        st.code(st.session_state.generated_query, language="sql" if db_type == "sql" else "json")

        if st.button("Execute Query"):
            try:
                if db_type == "sql":
                    result = execute_sql(st.session_state.generated_query)
                else:
                    result = execute_mongo(st.session_state.generated_query)
                st.dataframe(result)
            except Exception as e:
                st.error(f"Error executing query: {e}")
