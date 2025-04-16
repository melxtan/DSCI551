# Configuration file
import os

DATABASE_URL = os.getenv("DATABASE_URL", "your_database_url")
LLM_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "8f7f30756b2f44eaa303ea8f6e4b18fb")
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "https://gerhut.openai.azure.com/")
DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "gpt-4o")

def get_config():
    """Returns the configuration settings."""
    return {
        "DATABASE_URL": DATABASE_URL,
        "LLM_API_KEY": LLM_API_KEY,
        "ENDPOINT_URL": ENDPOINT_URL,
        "DEPLOYMENT_NAME": DEPLOYMENT_NAME,
    }