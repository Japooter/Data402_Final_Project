import pyodbc
server = '127.0.0.1' # Replace this with the actual name of your SQL Edge Docker container
username = 'sa' # SQL Server username
password = 'LrV_03121987!' # Replace this with the actual SA password from your deployment
database = 'FinalProject' # Replace this with the actual database name from your deployment. If you do not have a database created, you can use Master database.
db_connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=" + server + ";Database=" + database + ";UID=" + username + ";PWD=" + password + ";"
conn = pyodbc.connect(db_connection_string, autocommit=True)