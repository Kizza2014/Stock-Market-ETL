import pandas as pd
import pyodbc
from datetime import date
import pandas as pd
from dotenv import load_dotenv
import os


# establish connection to database
load_dotenv()
DRIVER = os.getenv("SQL_DRIVER")
SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("DATABASE")
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
conn = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={LOGIN};PWD={PASSWORD}')
cursor = conn.cursor()


# query database and return results as list of dictionaries
def to_dicts(query_results):
    """
        Convert query results from list of rows to list of dicts
    """
    descriptions = query_results[0].cursor_description
    columns = [description[0] for description in descriptions]
    result_as_dicts = []
    for result in query_results:
        result_as_dicts.append({attribute: value for attribute, value in zip(columns, result)})
    return result_as_dicts


def query_db(table_name):
    QUERY = f"SELECT * FROM {table_name}"
    cursor.execute(QUERY)
    query_results = cursor.fetchall()
    print(f"Extracted {len(query_results)} records from table {table_name}.")
    return to_dicts(query_results)


# save data to csv format
def save_to_csv(data, filepath: str):
    df = pd.DataFrame(data=data)
    df.to_csv(filepath, index=False)


if __name__ == "__main__":
    TABLES  = ["Patients", "Physicians", "SurgicalEncounters"]
    for table_name in TABLES:
        query_results = query_db(table_name=table_name)
    
        # save data
        today = date.today().strftime("%Y_%m_%d")
        filepath = f"staging/{table_name}_{today}.csv"
        save_to_csv(data=query_results, filepath=filepath)

