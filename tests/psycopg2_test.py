import psycopg2

from src.connections import POSTGRES_CONNECTION_DETAILS

RAW_TABLE_NAME = "shreeji"
CLEANED_TABLE_NAME = "shreeji_cleaned"

with psycopg2.connect(**POSTGRES_CONNECTION_DETAILS) as connection:
    with connection.cursor() as cursor:
        # cursor.execute(f"""
        #     SELECT * FROM test_table
        #     WHERE time::timestamp > '2022-06-24 06:20:35.99';
        # """)

        # cursor.execute(f"""
        #     SELECT time::timestamp FROM test_table
        #     WHERE test_id = 2
        #     LIMIT 1;
        # """)
        # res = cursor.fetchall()[0][0]
        #
        # cursor.execute(f"""
        #     SELECT time::timestamp FROM test_table
        #     WHERE time::timestamp >= '{res}';
        # """)

        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{RAW_TABLE_NAME}';
        """)

        res = cursor.fetchall()
        cleaned_schema = [col[0] for col in res]
        print(cleaned_schema)

        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{CLEANED_TABLE_NAME}';
        """)
        res = cursor.fetchall()
        raw_schema = [col[0] for col in res]
        print(raw_schema)
