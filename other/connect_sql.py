import pandas as pd
import sqlalchemy as sa
 
connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=sa;'
    'PWD=password;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc", 
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)
 
# # Deleting existing data in SQL Table:-
# with engine.begin() as conn:
#     conn.exec_driver_sql("DELETE FROM SchemaName.TableName")
 
# upload the DataFrame
academy_data.to_sql("TableName", engine, schema="dbo", if_exists="append", index=False)

