import streamlit as st
import pandas as pd
from utils import (connect_to_postgres, execute_nosql, execute_postgres, execute_sql, validate_sql, extract_sql_from_response, generate_query, get_nosql_schema, get_postgres_schema, get_sql_schema, generate_example_questions, clean_mongodb_data)

def summarize_schema(schema: dict, db_type: str) -> str:
    """Generates a friendly summary of the schema for non-technical users."""
    summary = []
    for table, columns in schema.items():
        column_list = ", ".join(columns)
        entity_name = "collection" if db_type == "mongodb" else "table"
        description = f"**{table}** {entity_name} with fields: {column_list}"
        summary.append(description)
    return "\n\n".join(summary)

def main():
    st.title("Natural Language to SQL/NoSQL Query")

    # Select DB type
    db_choice = st.radio("Select Database Type", ("MySQL", "PostgreSQL", "MongoDB"))

    # Fetch schema
    if db_choice == "MySQL":
        schema = get_sql_schema()
        db_type_code = "mysql"
    elif db_choice == "PostgreSQL":
        schema = get_postgres_schema()
        db_type_code = "postgres"
    else:
        schema = get_nosql_schema()
        db_type_code = "mongodb"

    # Display friendly schema summary
    st.write("### Database Schema Summary")
    st.markdown(summarize_schema(schema, db_type_code))

    # Optionally show raw JSON
    with st.expander("Show raw schema (advanced users)"):
        st.json(schema)

    # Get user query
    user_query = st.text_area("Enter your question in natural language")

    if "generated_query" not in st.session_state:
        st.session_state.generated_query = None

    if st.button("Generate Query"):
        if user_query:
            _, generated_query = generate_query(user_query, db_type_code)
            st.session_state.generated_query = generated_query

    # Display the generated query **only if it exists**
    if st.session_state.generated_query:
        st.write("### Generated Query:")
        st.code(st.session_state.generated_query, language="sql" if db_choice in ["MySQL", "PostgreSQL"] else "json")

        # Execute Query Button
        if st.button("Execute Query"):
            try:
                if db_choice == "MySQL":
                    if validate_sql(st.session_state.generated_query):
                        result = execute_sql(st.session_state.generated_query)
                    else:
                        st.error("Invalid MySQL query. Please try again.")
                        return
                elif db_choice == "PostgreSQL":
                    result = execute_postgres(st.session_state.generated_query)
                else:  # MongoDB
                    result = execute_nosql(st.session_state.generated_query)
                    # Clean MongoDB result
                    if isinstance(result, list):
                        result = [clean_mongodb_data(item) for item in result]
                    else:
                        result = clean_mongodb_data(result)
                
                # Convert result to DataFrame and handle data types
                if isinstance(result, list):
                    if result and isinstance(result[0], dict):
                        # If result is already a list of dictionaries, convert directly
                        result_df = pd.DataFrame(result)
                    else:
                        # Clean up the result format
                        cleaned_result = []
                        for item in result:
                            # Handle RealDictRow objects
                            if hasattr(item, 'items'):
                                cleaned_item = dict(item)
                            else:
                                # Flatten nested tuples completely
                                while isinstance(item, tuple):
                                    if len(item) == 1:
                                        item = item[0]
                                    else:
                                        break
                                
                                if isinstance(item, tuple):
                                    cleaned_item = list(item)
                                else:
                                    cleaned_item = [item]
                            cleaned_result.append(cleaned_item)
                        
                        # Create DataFrame with proper column names
                        if cleaned_result:
                            result_df = pd.DataFrame(cleaned_result)
                            # Use original column names if available
                            if isinstance(cleaned_result[0], dict):
                                result_df.columns = list(cleaned_result[0].keys())
                            else:
                                # Rename columns if they are numeric
                                result_df.columns = [f'Column_{i+1}' for i in range(len(result_df.columns))]
                        else:
                            result_df = pd.DataFrame()
                else:
                    result_df = pd.DataFrame([str(result)])
                
                # Display results
                if not result_df.empty:
                    st.dataframe(result_df)
                else:
                    st.write("No results found.")
                
                # Download button for CSV
                if not result_df.empty:
                    csv = result_df.to_csv(index=False).encode('utf-8')
                    st.download_button("Download Results", csv, "results.csv", "text/csv")
            except Exception as e:
                st.error(f"Error executing {db_choice} query: {str(e)}")

if __name__ == "__main__":
    main()
