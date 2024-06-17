import sys

from sqlalchemy import text

import cleaning_and_ingestion.clean_import as ci
import subprocess
import os
import sqlalchemy

dflt_driver = 'ODBC Driver 17 for SQL Server'
dflt_server_ip = '127.0.0.1'
dflt_db = 'FinalProject'
dflt_username = 'sa'
dflt_password = ''


def main(driver, server_ip, db, username, password):
    connection_string = (
        f'Driver={driver};'
        f'Server={server_ip};'
        f'Database={db};'
        f'UID={username};'
        f'PWD={password};'
        'Trusted_Connection=no;'
    )
    connection_url = sqlalchemy.engine.URL.create(
        "mssql+pyodbc",
        query=dict(odbc_connect=connection_string)
    )
    engine = sqlalchemy.create_engine(connection_url, fast_executemany=True)

    # Install requirements
    print("Installing required python libraries")
    requirements = os.path.join(os.getcwd(), "requirements.txt")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements])
    print("Completed installing required python libraries")

    print("Beginning extraction of data from S3")
    # Academy csv
    print("Processing academy csv data!")
    academy_data = ci.clean_academy_csv()
    ci.insert_into_sql(academy_data, engine, "Academy_CSV")
    print("Successfully inserted academy csv data!")

    # Talent json
    print("Processing talent json data!")
    talent_json = ci.clean_talent_json()
    ci.insert_into_sql(talent_json, engine, "Talent_JSON")
    print("Successfully inserted talent json data!")

    # Talent csv
    print("Processing talent csv data!")
    talent_csv = ci.clean_talent_csv()
    ci.insert_into_sql(talent_csv, engine, "Talent_CSV")
    print("Successfully inserted talent csv data!")

    # Talent txt
    print("Processing talent txt data!")
    talent_txt = ci.clean_talent_txt()
    ci.insert_into_sql(talent_txt, engine, "Talent_TXT")
    print("Successfully inserted talent txt data!")

    # Create Score table
    academy_data_melted = ci.create_score_data(academy_data)
    ci.insert_into_sql(academy_data_melted, engine, "Score")

    return


if __name__ == "__main__":
    args = sys.argv[1:]
    #get database server ip
    try:
        ind_server = args.index("-server") + 1
        dflt_server_ip = args[ind_server]
    except:
        pass
    # Get database name
    try:
        ind_db = args.index("-db") + 1
        dflt_db = args[ind_db]
    except:
        pass

    #get username
    try:
        ind_un = args.index("-user") + 1
        dflt_username = args[ind_un]
    except:
        pass
    ##Get password
    try:
        ind_pw = args.index("-password") + 1
        dflt_password = args[ind_pw]
    except:
        pass
    main(dflt_driver, dflt_server_ip, dflt_db, dflt_username, dflt_password)
