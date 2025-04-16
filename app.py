import streamlit as st
import pandas as pd
from utils import generate_query, execute_sql, execute_mongo, get_sql_schema, get_nosql_schema

def main():
    st.title("Natural Language to Database Query")

    db_choice = st.radio("Select Database Type", ("SQL", "NoSQL"))
    db_type = "sql" if db_choice == "SQL" else "nosql"

    # Dynamically fetch schema from live database
    with st.expander("View Database Schema"):
        try:
            schema = get_sql_schema() if db_type == "sql" else get_nosql_schema()
            if not schema:
                st.warning("No tables or collections found.")
            else:
                for table, columns in schema.items():
                    st.subheader(table)
                    st.markdown(", ".join(f"`{col}`" for col in columns))
        except Exception as e:
            st.error(f"Failed to load schema: {e}")

    # Example natural language questions for user inspiration
    with st.expander("Example Questions You Can Ask"):
        st.markdown("""
        - What is the population of Tokyo?
        - List all official languages spoken in France.
        - Which countries in Europe have a population over 50 million?
        - Show all cities in Japan with more than 1 million people.
        - What is the capital of Brazil?
        """)

    user_query = st.text_area("Enter your query in natural language")

    if "generated_query" not in st.session_state:
        st.session_state.generated_query = None

    if st.button("Generate Query"):
        if user_query:
            try:
                st.session_state.generated_query = generate_query(user_query, db_type)
            except Exception as e:
                st.error(f"Error generating query: {e}")

    if st.session_state.generated_query:
        st.write("Generated Query:")
        st.code(st.session_state.generated_query, language="sql" if db_type == "sql" else "json")

        if st.button("Execute Query"):
            try:
                result = (
                    execute_sql(st.session_state.generated_query)
                    if db_type == "sql"
                    else execute_mongo(st.session_state.generated_query)
                )
                st.dataframe(result)
            except Exception as e:
                st.error(f"Error executing query: {e}")

if __name__ == "__main__":
    main()
