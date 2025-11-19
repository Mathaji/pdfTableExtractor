import tabula
import time
from post_in_database import create_database as database

INPUT_FILE = ""

max_tables = 0
START_TIME = 0
END_TIME   = 0


def convert_to_json(tables, columns):
    global max_tables
    global START_TIME
    global END_TIME

    transaction_list = []

    for table_number, table in enumerate(tables):
        max_tables = len(tables)
        
        if len(table.columns) == len(columns): 
            table.columns = columns
            rows_in_table = table.iloc[0:]
            json_table = rows_in_table.to_dict(orient='records')
            transaction_list.append(json_table)
    return transaction_list
            
def main():

    file     = input("Paste the file location: ")
    pdf_password = input("Enter your PDF password: ")

    tables = tabula.read_pdf(file,
                            encoding="latin-1",
                            pages="all",
                            multiple_tables=True,
                            pandas_options={'header': None},
                            password=pdf_password,
                            format="JSON")

    columns = []
    column_number = 1

    while True:
        name = input(f"Enter column {column_number} name (Press 'done' to finish): ").lower()
        column_name = name.replace(' ', '_').replace('.', '').strip()

        if column_name.lower() == "done":
            print(f"{len(columns)} column name(s) saved")
            break
        columns.append(column_name)
        column_number += 1
    
    start = time.time()

    data_in_json = convert_to_json(tables, columns)

    end = time.time()
    print(f"Extraction time: {end - start:.2f}(second(s))\n\n\n")


    data_type        = []
    column_data_type = {}
    loop_count       = 0
    my_dict          = 0

    table_name = input("Enter Table name: ").lower().strip()

    while True:
        for value in columns:
            value = input(f"Column {value}, enter data type: ").strip().capitalize()
            data_type.append(value)

            if loop_count == len(columns):
                break
            loop_count += 1

        for k, v in zip(columns, data_type):
            column_data_type[k] = v
            my_dict += 1

        if my_dict == len(column_data_type):
            break

    add_to_db_start = time.time()
    database(column_data_type, table_name, data_in_json)
    add_to_db_end = time.time()

    print(f"Add to Database time: {add_to_db_end - add_to_db_start:.2f}(second(s))\n\n\n")

    print(f"=========100% complete=======>")


main()