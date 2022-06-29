# Ideally Stored in an Environment Variable
POSTGRES_DBNAME = "shreeji"
POSTGRES_USER = "ezyloads"
POSTGRES_PASSWORD = "ezy@1234"
POSTGRES_HOST = "localhost"

# POSTGRES_CONNECTION_DETAILS = f"dbname={POSTGRES_DBNAME} user={POSTGRES_USER} " + \
#                               f"password={POSTGRES_PASSWORD} host={POSTGRES_HOST}"

POSTGRES_CONNECTION_DETAILS = {
    'dbname': POSTGRES_DBNAME,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'host': POSTGRES_HOST
}

RAW_TABLE_NAME = 'shreeji'
CLEANED_TABLE_NAME = 'shreeji_cleaned_test'

__all__ = [POSTGRES_CONNECTION_DETAILS, RAW_TABLE_NAME, CLEANED_TABLE_NAME]
