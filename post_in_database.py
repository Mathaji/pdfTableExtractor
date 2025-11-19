import psycopg2
import dotenv
import os
# import json

dotenv.load_dotenv()

DB_HOST = os.getenv("DATABASE_HOST")
DB_PW   = os.getenv("DATABASE_PASSWORD")
DB_PRT  = os.getenv("DATABASE_PORT")
DB_USER = os.getenv("DATABASE_USER")
DB_NAME = os.getenv("DATABASE_NAME")

def connect_to_db():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                                      password=DB_PW, host=DB_HOST,
                                      port=DB_PRT)
    except Exception as e:
        print(f"DB Connection failed: {e}")
    
    return conn


def create_database(columns:dict, table_name, json_data):

    connection = connect_to_db()

    with connection.cursor() as db_operations:
        SQL = f"CREATE TABLE IF NOT EXISTS {table_name}(id serial PRIMARY KEY);"
        
        db_operations.execute(SQL)
        
        column_list = []
        for column, data_type in columns.items():
           # data_type = new_type
        
            add_columns = f"Alter TABLE {table_name} ADD COLUMN IF NOT EXISTS {column} {data_type};"
            db_operations.execute(add_columns)
            column_list.append(column)
            connection.commit()

        insert_in_db(json_data, table_name, column_list)

def insert_in_db(json_data, table_name, columns):
    connection = connect_to_db()

    
    list_of_columns = set(columns)
    cleaned_list = list(list_of_columns)

    with connection.cursor() as db_operations:
        for multiple_transactions in json_data:
            for page_transactions in multiple_transactions:
                print(f"{page_transactions}\n==================================\n")

                # for transactions in page_transactions:
                # values = []
                # for key in cleaned_list:
                #     # print(f"{page_transactions}\n======================\n")
                #     values.append(page_transactions[key])

                # print(f"Values: {values}")
                

                # insert_into = f"INSERT INTO {table_name} ({', '.join(cleaned_list)}) VALUES({', '.join([repr(v) for v in values])})" # .replace('nan', 'null').replace('-', 'null')
                # db_operations.execute(insert_into)
                # connection.commit()
