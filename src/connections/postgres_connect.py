import os
from dotenv import load_dotenv
load_dotenv()

"""
This is the main file for the Postgres connection.

Assumes that the following environment variables are set in the environment
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_HOST
POSTGRES_DBNAME
"""

POSTGRES_CONNECTION_DETAILS = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST')
}

RAW_TABLE_NAME = 'shreeji'
CLEANED_TABLE_NAME = 'shreeji_cleaned_test'

__all__ = [POSTGRES_CONNECTION_DETAILS, RAW_TABLE_NAME, CLEANED_TABLE_NAME]
