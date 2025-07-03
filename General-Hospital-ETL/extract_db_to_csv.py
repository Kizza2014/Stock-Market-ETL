import pandas as pd
import pyodbc
from datetime import date
import pandas as pd

driver = "{ODBC Driver 17 for SQL Server}"
server = 'DESKTOP-DMC84B8\SQLEXPRESS'
database = "GeneralHospital"
login = "test3"
password = "1"

conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={login};PWD={password}')

cursor = conn.cursor()

SQL_QUERY = """
    SELECT *
    FROM PATIENTS;
"""

cursor.execute(SQL_QUERY)

res = cursor.fetchall()
print(type(res[0]))
print(res[0].cursor_description)