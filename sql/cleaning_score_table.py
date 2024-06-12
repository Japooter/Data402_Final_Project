import numpy as np
import pandas as pd
import sqlalchemy as sa

connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=sa;'
    'PWD=yourStrong(!)Password;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)

academy_data = pd.read_sql("SELECT * FROM Academy_CSV",engine)


academy_data_melted = academy_data.melt(id_vars=['Category', 'Stream', 'Date', 'ACname', 'trainer'], var_name='variable', value_name='Score')
academy_data_melted['Week'] = academy_data_melted['variable'].str.extract(r'_(W\d+)$')
academy_data_melted['Behaviour'] = academy_data_melted['variable'].str.extract(r'^(\w+)_')
academy_data_melted = academy_data_melted.drop(columns=['variable'])


academy_data_melted.to_sql('Score', engine, schema='dbo', if_exists='replace', index=False)