# Data 402 Final Project
![Alt text](images/images.png)


## Table of Contents
- [Introduction](#introduction)
- [Methodology](#methodology)
- [Setup instructions](#setup-instructions)
- [Extraction](#extraction)
- [Transformation](#transformation)
- [Loading](#loading)
- [Data Schema](#data-schema)
- [Testing](#testing)
- [Licensing](#licensing)
- [Collaborators](#collaborators)
- [Acknowledgements](#acknowledgements)


## Introduction

### Project overview:

**Task:** 

Create an ETL pipeline using mock data from a fictionalised version of Sparta Global and generate some analytical insights to present to stakeholders. 

**Task Requirements:**

- The code needs to be at production-level (easily reusable). 

- Have an appropriate data store that will house all the data drawn from the files, the database should provide a single-person view.

**Prerequisites:**

The following requirements have to be met for the ETL pipeline to run successfully:

- Ensure the latest version of Python has been installed. 

- Ensure you have PyCharm, Docker and Azure Studio installed and running correctly. 

- Required Python libraries to import: boto3, pandas, NumPy, pytest, json, SQL alchemy (requirements.txt). 

- Credentials to access the ‘data-402-final-project’ S3 bucket needs to be requested from Sparta Global. 

### Data Pipeline Diagram
To provide a clear overview of the ETL process, below is a diagram illustrating the various steps involved in the pipeline:

![Alt text](images/pipeline.drawio.png)

## Methodology:

Our team adopted Agile methodologies, facilitated through Microsoft Teams for all meetings and communication. The Scrum Master sent out calendar invites for all ceremonies, ensuring everyone was aware of scheduled activities. Here's a breakdown of our Agile practices:

- **Daily Stand-ups:** Conducted every morning to discuss progress, obstacles, and plans for the day. We reviewed *user stories* daily to ensure alignment and progress.
- **Sprint Planning:** Held daily to plan tasks for the sprint, ensuring a clear roadmap for the upcoming work.
- **Sprint Reviews:** Conducted daily or every 1.5 days, depending on our progress, to review completed work and gather feedback.
- **Retrospectives:** Held regularly to reflect on our process and identify areas for improvement.
- **User Stories:** Managed using *Microsoft Planner*, where we documented acceptance criteria and checked off tasks upon meeting the definition of done.
- **Role Rotation:** Team members took turns acting as Scrum Master and Product Owner, rotating *every two days*, as seen in the table below:<br>


![img.png](img.png)

- **Stakeholder Interaction:** *Trainers acted as stakeholders*, providing feedback and direction.
- **Subgroup Collaboration:** We broke into subgroups to tackle specific tasks before reconvening for updates and integration.
- **Project Overview:** Maintained a project overview document on *SharePoint*, accessible to all team members for reference and updates.

## Challenges:

1. **Concurrent Responsibilities:** Balancing work on our Final Quality Gate presentation and interview preparation alongside the project was challenging.
2. **Lack of Dedicated Roles:** The absence of a dedicated Product Owner and Scrum Master sometimes led to role-related inefficiencies.
3. **Data Cleaning Issues:** We encountered inaccuracies in our initial data cleaning, stemming from inconsistent formats and structures (e.g., lists and dictionaries instead of strings). This required us to fix the issues and re-run the pipeline.
4. **Communication Hurdles:** Early on, we faced issues with team members talking over each other in meetings. This was quickly resolved by utilising the 'raise hand' feature in Teams, facilitated by the Scrum Master.

By addressing these challenges and continuously refining our process, we were able to maintain productivity and deliver on our project goals.


## Setup instructions 

### 1. Install required software

- Install Python: [Python Releases | Python.org ](https://www.python.org/downloads/)
- Install Docker: [Install Docker Engine | Docker Docs](https://docs.docker.com/engine/install/)<br>
- Install Azure Data Studio: [Download and install Azure Data Studio - Azure Data Studio | Microsoft Learn](https://learn.microsoft.com/en-us/azure-data-studio/download-azure-data-studio)


### 2. Install ODBS Driver

- Install ODBS 17 driver: [Download ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#version-17)

### 3. Setup Azure SQL Edge Docker container

***Note:*** *If you are not on Windows, preface these commands with sudo*
- Pull the Azure SQL Edge container
 `docker pull mcr.microsoft.com/azure-sql-edge:latest`

- Deploy the container using
 `docker run --cap-add SYS_PTRACE -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=yourStrong(!)Password -p 1433:1433 --name azuresqledge -d mcr.microsoft.com/azure-sql-edge`

### 4. Connect with Azure Data Studio

- Open Azure Data Studio.
- Create a Connection.
- Use details:
    - Connection type: `Microsoft SQL Server`
    - Server: `localhost,1433`
    - Authenitcation type: `SQL Login`
    - User name: `sa`
    - Password: `yourStrong(!)Password`
    - Trust server certificate: `True`
- After connecting, run the code `CREATE DATABASE FinalProject;`


### 5. Clone the project repository to your local machine

- `$ git clone https://github.com/Japooter/Data402_Final_Project.git`


### 6. Install the required python libraries

- `pip install -r requirements.txt`

### 5. Run scripts
- First run the main.py file with your parameters
    - `python -m main -server 127.0.0.1 -db FinalProject -user sa -password yourStrong(!)Password`
- Open the file [sql/Complete_Code.sql](sql/Complete_Code.sql) in Azure Data Studio.
- Run the Complete_Code.sql file.

### 6. Congratulations, you now have the complete database setup!

## Extraction
The Sparta global mock data was extracted from the Amazon S3 `data-402-final-project` bucket which contained two further prefixes: Talent and Academy. 

**Talent contents**: 
<br>Data collected on the Sparta day event, contains information about candidate competency, the outcome of behaviour and their performance and various recruitment tests.
  - `.json`
  - `.csv`
  - `.txt`  

**Academy contents**:
<br>Data collected from all the Spartans in the academy, with their names, courses and what score they were getting in their weekly tests.
  - `.csv` 

## Transformation

Image to show which columns are found in the different files:

![Alt text](images/files.png)

### JSON files: 

- The Talent JSON files were loaded into a pandas DataFrame.

- NaN values were identified  in the tech_self_score column and were converted into an empty dictionary string, ‘{}'. 

- We also converted the strengths and weaknesses columns to strings. 

- Duplicate rows were identified and dropped. 

### Academy CSV files: 

- The academy CSV files were loaded into a pandas DataFrame.

- Whitespace was cleaned before and after the entry of each string. 

- The category, stream name and start data from each CSV filenames were extracted and assigned to corresponding candidates. 

- Checked for duplicate and null values but there were none. 

### Talent CSV files: 

- The talent CSV files were loaded into a pandas DataFrame.

- Special characters such as "-", " ", "(", and ")" were removed from the phone number column. 

- All the month names were standardised to the format "full month name-year" (e.g., "September-2019"). 

- The ‘invite_date’ (date of the month of their Sparta day) and ‘month’ was merged into a single column specifying ‘sparta_day_date’, ensuring it is of type ‘datetime.date’. 

- The ‘invite_date’ and ‘month’ columns were dropped. 

- The ‘dob’ (date of birth) column was enforced with the ‘datetime.date’ type for proper date interpretation. 

- Ensured the correct capitalisation of names in "First Last" format and street addresses. 

- All the leading and trailing whitespaces for all columns were removed. 


### Talent TXT files: 

- The talent txt files were loaded into a pandas DataFrame. 

- Whitespace characters were removed from the ‘date’, ‘name’ and ‘academy' column.

- The ‘psychometric_score’ and ‘presentation_score’ columns were converted into the integer data type.

- Checked for duplicate and null values but there were none. 

- The ‘date’ column was converted into the ‘datetime.date’ format.

## Loading

After extracting and transforming the data, the final step in our ETL pipeline is loading the cleaned data into our database for further analysis and reporting. This process involves the following steps:

### 1. Set up the Database:

- We used **Azure Data Studio** to create a database named **FinalProject**. This database is hosted on a **SQL Edge** server.

### 2. Docker Container:

- A **Docker** container was run to host the SQL Edge server. The connection details were configured as follows:

```
connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=<username>;'
    'PWD=<password>>;'
    'Trusted_Connection=no;'
)
```

### 3. Python Script:

  - A **Python** script was used to load the transformed data into the SQL database. This script reads the data from four cleaned dataframes (**Talent_CSV**, **Talent_TXT**, **Talent_JSON**, **Academy_CSV**) and inserts them into the FinalProject database as individual tables.

Here is a high-level overview of the Python script:

- Dependencies:

  The script utilises various libraries including:
    - **boto3** for **S3** interactions
    - **pandas** for data manipulation
    - **sqlalchemy** for database operations

- Connection Setup:

  - The script sets up a connection to the SQL database using SQLAlchemy.

- Data Loading Functions:

  - Four functions are defined to load and clean data from different sources (JSON, CSV, TXT):

    - `clean_academy_csv()`: Loads and cleans data from the Academy CSV files.
    - `clean_talent_json()`: Loads and cleans data from the Talent JSON files.
    - `clean_talent_csv()`: Loads and cleans data from the Talent CSV files.
    - `clean_talent_txt()`: Loads and cleans data from the Talent TXT files.
    

- Insert Data into SQL:

  - The cleaned dataframes are inserted into the SQL database using the `insert_into_sql()` function, which uses the `to_sql` method from pandas to perform the insertion.
  

- Main Execution:

  - The script's main block orchestrates the loading process, invoking the cleaning functions and then inserting the results into the database tables.

```commandline
import boto3
import datetime
import json
import re
from io import StringIO

import numpy as np
import pandas as pd
import sqlalchemy as sa

# Set up S3 client
s3 = boto3.client('s3')

# Connection string to SQL Server
connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=<username>;'
    'PWD=<password>;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)

# Function definitions for loading and cleaning data...

# Insert data into SQL function
def insert_into_sql(dataframe, engine, tablename):
    dataframe.to_sql(tablename, engine, schema="dbo", if_exists="replace", index=False)
    return

if __name__ == "__main__":
    # Process Academy CSV data
    print("Processing academy csv data!")
    academy_data = clean_academy_csv()
    insert_into_sql(academy_data, engine, "Academy_CSV")
    print("Successfully inserted academy csv data!")

    # Process Talent JSON data
    print("Processing talent json data!")
    talent_json = clean_talent_json()
    insert_into_sql(talent_json, engine, "Talent_JSON")
    print("Successfully inserted talent json data!")

    # Process Talent CSV data
    print("Processing talent csv data!")
    talent_csv = clean_talent_csv()
    insert_into_sql(talent_csv, engine, "Talent_CSV")
    print("Successfully inserted talent csv data!")

    # Process Talent TXT data
    print("Processing talent txt data!")
    talent_txt = clean_talent_txt()
    insert_into_sql(talent_txt, engine, "Talent_TXT")
    print("Successfully inserted talent txt data!")
```
### 4. Normalization and Relationships:

- After loading the data into the SQL database, the data was further normalized to ensure efficiency and integrity. **Primary Keys** (PKs), **Foreign Keys** (FKs), and **junction tables** were established to create relationships between the data tables, ensuring a single view of each candidate.

By following this process, we ensure that our data is stored in a well-structured and efficient manner, ready for any further analysis and reporting required.



## Data Schema

The database schema for the Final Project ETL pipeline consists of multiple interconnected tables designed to store and manage the extracted and transformed data efficiently. Below is a description of each table, including its columns and relationships:

### Entity Relationship Diagram
![Alt text](images/erd.png)

### APPLICANTS

- `applicant_ID (PK)`: Unique identifier for each applicant.
- `sparta_day_ID (FK)`: References `sparta_day_ID` in the `SPARTA_DAY` table.
- `self_development`: Self-development score.
- `geo_flex`: Geographic flexibility indicator.
- `financial_support_self`: Financial self-support indicator.
- `result`: Result of the application process.
- `course_interest`: Interested course.
- `gender`: Gender of the applicant.
- `psychometric_score`: Psychometric score.
- `presentation_score`: Presentation score.
- `dob`: Date of birth.
- `email`: Email address.
- `address_ID (FK)`: References `address_ID` in the `ADDRESS` table.
- `phone_number`: Phone number.
- `uni_ID (FK)`: References `uni_ID` in the `UNI` table.
- `degree_grade`: Degree grade.
- `talent_coordinator_ID`: Identifier for the talent coordinator.

### CATEGORIES
- `category_id (PK)`: Unique identifier for each category.
- `category_name`: Name of the category.

### TRAINERS
- `trainer_id (PK)`: Unique identifier for each trainer.
- `name`: Name of the trainer.

### STREAMS
- `stream_ID (PK)`: Unique identifier for each stream.
- `category_id (FK)`: References `category_id` in the `CATEGORIES` table.
- `trainer_ID (FK)`: References `trainer_id` in the `TRAINERS` table.
- `start_date`: Start date of the stream.

### STREAMS_JUNCTION
- `stream_ID (FK)`: References `stream_ID` in the `STREAMS` table.
- `applicant_ID (FK)`: References `applicant_ID` in the `APPLICANTS` table.

### LOCATION
- `location_ID (PK)`: Unique identifier for each location.
- `location`: Name of the location.

### SPARTA_DAY
- `sparta_day_ID (PK)`: Unique identifier for each Sparta day.
- `date`: Date of the Sparta day.
- `location_ID (FK)`: References `location_ID` in the `LOCATION` table.

### ADDRESS
- `address_ID (PK)`: Unique identifier for each address.
- `address_name`: Name of the address.
- `city_name`: Name of the city.
- `postcode_name`: Postcode of the address.

### UNI
- `uni_ID (PK)`: Unique identifier for each university.
- `name`: Name of the university.

### TECH_SKILL_APPLICANT
- `applicant_ID (PK, FK1)`: References `applicant_ID` in the `APPLICANTS` table.
- `tech_skill_ID (PK, FK2)`: References `tech_skill_ID` in the `TECH_SKILL` table.
- `score`: Skill score for the applicant.

### TECH_SKILL
- `tech_skill_ID (PK)`: Unique identifier for each technical skill.
- `tech_skill_name`: Name of the technical skill.

### SCORE
- `applicant_ID (FK)`: References `applicant_ID` in the `APPLICANTS` table.
- `assessors_ID (FK)`: References `assessors_ID` in the `ASSESSORS` table.
- `week`: Week number.
- `score`: Score for the week.

### ASSESSORS
- `assessors_ID (PK)`: Unique identifier for each assessor.
- `name`: Name of the assessor.

### WEAKNESS_JUNCTION
- `applicant_ID (FK)`: References `applicant_ID` in the `APPLICANTS` table.
- `weakness_ID (FK)`: References `weakness_ID` in the `WEAKNESSES` table.

### WEAKNESSES
- `weakness_ID (PK)`: Unique identifier for each `weakness_name`.
- `weakness_name`: Name of the weakness.

### STRENGTH_JUNCTION
- `applicant_ID (FK)`: References `applicant_ID` in the `APPLICANTS` table.
- `strength_ID (FK)`: References `strength_ID` in the `STRENGTHS` table.

### STRENGTHS
- `strength_ID (PK)`: Unique identifier for each `strength_name`.
- `strength_name`: Name of the strength.

### TALENT_COORDINATORS
- `talent_coordinator_ID (PK)`: Unique identifier for each talent coordinator.
- `name`: Name of the talent coordinator.


## Relationships
`APPLICANTS` is related to `SPARTA_DAY`, `ADDRESS`, and `UNI` through foreign keys.

`STREAMS` is related to `CATEGORIES` and `TRAINERS` through foreign keys.

`STREAMS_JUNCTION` serves as a junction table linking `STREAMS` and `APPLICANTS`.

`SPARTA_DAY` is related to `LOCATION` through a foreign key.

`TECH_SKILL_APPLICANT` serves as a junction table linking `APPLICANTS` and `TECH_SKILL`.

`SCORE` links `APPLICANTS` and `ASSESSORS`.

`WEAKNESS_JUNCTION` and `STRENGTH_JUNCTION` serve as junction tables linking `APPLICANTS` to their respective strengths and weaknesses.

This schema ensures that all relevant data is stored in a normalized form, facilitating efficient querying and data integrity.

## SQL Normalization Explanation

In this section, we provide an overview of the SQL code used for normalizing our data. Normalization is essential to reduce redundancy and ensure data integrity.

### Normalization Process

Normalization typically involves organizing the columns and tables of a database to minimize redundancy and dependency. We followed standard normalization forms, primarily focusing on achieving the Third Normal Form (3NF).

**Function that separates values that have are in a list format, so that our data is atomic.**

![Alt text](images/k1.png)

**Use that function to make a unique set of strengths, same is done for weaknesses.**

![Alt text](images/k2.png)

**As the comment mentions, altering the original data table columns for easier joins.**

![Alt text](images/k3.png)

**Joining all data via name for further data exploration.**

![Alt text](images/k4.png)


**Removing any columns that are irrelevant to match the schema.**

![Alt text](images/k5.png)


**Adding a primary for unique indexing.**

![Alt text](images/k6.png)

**Creating relevant junction tables for the applicant's strengths and weaknesses.**

![Alt text](images/k7.png)

**Creating additional tables and creating foreign keys based on the ERD diagram and then updating the 'Applicants' table to be referencing the created tables via primary key - foreign key relationships.**

![Alt text](images/k8.png)



## Testing

Testing is an integral part of ensuring the reliability and correctness of the ETL pipeline developed for the Data 402 Final Project. Our testing strategy encompasses various aspects of the pipeline, including data extraction, transformation, loading processes, as well as ensuring robust error handling for different scenarios.

### Test Cases:
### i. Data Extraction

1. ### Incorrect Bucket Name (Academy Data)

-  **Description**: Tests handling of exceptions when an incorrect bucket name is provided for Academy data extraction.
-  **Test Code**: `test_incorrect_bucket_academy()`

2. ### Incorrect Bucket Name (Talent JSON Data)

- **Description**: Tests handling of exceptions when an incorrect bucket name is provided for Talent JSON data extraction.
-  **Test Code**: `test_incorrect_bucket_talent_json()`

3. ### Missing Prefix (Talent JSON Data)

-  **Description**: Tests handling of scenarios where no prefix is found in the S3 bucket for Talent JSON data extraction.
-  **Test Code**: `test_no_prefix_bucket_talent_json()`

4. ### Incorrect Bucket Name (Talent TXT Data)

-  **Description**: Tests handling of exceptions when an incorrect bucket name is provided for Talent TXT data extraction.
-  **Test Code**: `test_incorrect_bucket_talent_txt()`

5. ### Incorrect Bucket Name (Talent CSV Data)

-  **Description**: Tests handling of exceptions when an incorrect bucket name is provided for Talent CSV data extraction.
-  **Test Code**: `test_incorrect_bucket_talent_csv()`

### ii. **Data Transformation**

6. ### Whitespace Cleaning

-  **Description**: Tests the function `clean_whitespace()` for removing leading and trailing whitespaces.
-  **Test Code**: `test_whitespace()`

7. ### Month Cleaning

-  **Description**: Tests the function `clean_month()` for standardizing month names.
-  **Test Code**: `test_month_clean()`

8. ### Phone Number Formatting

-  **Description**: Tests the function `clean_phone_numbers()` for cleaning and formatting phone numbers.
-  **Test Code**: `test_number()`

9. ### Date of Birth Conversion

-  **Description**: Tests the function `dobs_to_datetime()` for converting date strings to `datetime.date` objects.
-  **Test Code**: `test_dobs_conv()`

10. ### Capitalization

-  **Description**: Tests the function `capitalise()` for capitalizing strings correctly.
-  **Test Code**: `test_capitalisation()`

### iii. **SQL Operations**

11. ### Insertion into SQL Database
-  **Description**: Tests the function `insert_into_sql()` for inserting data into the SQL database.
-  **Test Code**: `test_wrong_sql_info()`


### iv. **Additional Functionalities**

12. ### Date Parsing

-  **Description**: Tests the function get_date() for parsing dates from string formats.
-  **Test Code**: test_get_date()

13. ### Stream Extraction

-  **Description**: Tests the function `get_stream()` for extracting stream names from filenames.
-  **Test Code**: `test_get_stream()`

14. ### Category Extraction

-  **Description**: Tests the function `get_category()` for extracting category names from filenames.
-  **Test Code**: `test_get_category()`

### Testing Code:
```commandline
from clean_import import *

# s3 = boto3.client('s3')
def test_incorrect_bucket_academy():
    with pytest.raises(Exception) as e_info:
        load_academy_data('bad-bucket-name', 'Academy/')

def test_incorrect_bucket_talent_json():
    with pytest.raises(Exception) as e_info:
        check_talent_json_files('where-are-the-jsons', 'Talent/')

def test_no_prefix_bucket_talent_json():
    actual = check_talent_json_files('data-402-final-project', 'Talentless/')
    expected = None
    assert actual == expected
    # with pytest.raises(Exception) as e_info:
    #     check_talent_json_files('where-are-the-jsons', 'Talent/')

def test_incorrect_bucket_talent_txt():
    with pytest.raises(Exception) as e_info:
        list_txt_files('where-are-the-jsons', 'Talent/')

def test_incorrect_bucket_talent_csv():
    with pytest.raises(Exception) as e_info:
        load_talent_csv_data('where-are-the-jsons', 'Talent/')


print(clean_whitespace('gherkins and pickles are similar  '))
def test_whitespace():
    actual = clean_whitespace('  gherkins and pickles are similar  ')
    expected = 'gherkins and pickles are similar'
    assert actual == expected

def test_month_clean():
    # actual = clean_month('Sept2025')
    # expected = 'September-2025'
    actual = clean_month('Oct2025')
    expected = 'Oct-2025'
    assert actual == expected

def test_number():
    actual = clean_phone_numbers('9023-244 234 (344)')
    expected = ('9023244234344')
    assert actual == expected

def test_comb_time():
    actual = combine_date_and_month('2323-23-23', 'September')
    expected = 'September'
    assert actual == expected

def test_dobs_conv():
    actual = dobs_to_datetime('29/05/2020')
    expected = datetime.datetime.strptime('29/05/2020', '%d/%m/%Y').date()
    assert actual == expected

def test_capitalisation():
    actual = capitalise('the grand pizza fight')
    expected = 'The Grand Pizza Fight'
    assert actual == expected

def test_wildcards():
    actual = remove_wildcards('H4ell&45350o,!')
    expected = 'Hello'
    assert actual == expected

def test_wrong_sql_info():
    with pytest.raises(Exception) as e_info:
        insert_into_sql('this', 'is', 'wrong')

def test_get_date():
    actual = get_date('Business_20_2027-02-11')
    expected = datetime.date(2027, 2, 11)
    assert actual == expected

def test_get_stream():
    actual = get_stream('Business_20_2019-02-11')
    expected = 'Business20'
    assert actual == expected

def test_get_category():
    actual = get_category('Business_20_2019-02-11')
    expected = 'Business'
    assert actual == expected


import pytest
from unittest.mock import MagicMock
from moto import mock_aws

# Mock s3 and list_all_objects
s3 = MagicMock()
list_all_objects = MagicMock()


@pytest.fixture
def test_s3_boto():
    s3 = boto3.client('s3', region_name='us-east-1')
    return s3


@mock_aws
def test_access_denied(test_s3_boto):
    with pytest.raises(Exception) as e_info:
        bucket = "testbucket"
        key = "testkey"
        body = "testing"
        test_s3_boto.create_bucket(Bucket=bucket)
        test_s3_boto.put_object(Bucket=bucket, Key=key, Body=body)
        ls_output = list_all_objects(Bucket=bucket, Prefix=key)
        assert len(ls_output) == 1
        assert ls_output[0] == 'testkey'
```


### Test Environment Setup:
For testing, we utilised *pytest* as our testing framework, leveraging its capabilities to run unit tests and manage test fixtures effectively. Each test case was designed to validate specific functionalities and edge cases within our ETL pipeline, ensuring robustness and reliability across different scenarios.


## Collaborators:


| Name                                                |
|-----------------------------------------------------|
| Dafydd Lloyd (Scrum Master/Product Owner/Developer) |
| Jacqueline Onyemechi Ochonma (Developer)            |
| James O'Brien (Product Owner/Developer)             |
| Kazim Raza (Product Owner/Developer)                |
| Kehinde Giwa (Developer)                            |
| Kyrun Philip-Lessells (Product Owner/Developer)     |
| Sabrina Kaur (Scrum Master/Developer)               |
| Samuel Smith (Scrum Master/Developer)               |
| Yoonhee Lee (Scrum Master/Developer)                |
| Luis Rodriguez Valido (Developer)                   |
| Rajpal Aujla (Developer)                            |  

## Licensing:

This project is the intellectual property of Sparta Global. As trainee data engineers at Sparta Global, our work including data extraction, transformation, and loading (ETL) processes utilised resources from Sparta Global's infrastructure, including the use of their S3 bucket for data storage. All rights, including but not limited to intellectual property rights, remain with Sparta Global.

For licensing inquiries or further information, please contact Sparta Global directly.

## Acknowledgements:

We would like to extend our gratitude to the following individuals for their invaluable contributions to this project:

### Data402 Group Members at Sparta Global
- Dafydd Lloyd
- Jacqueline Onyemechi Ochonma
- James O'Brien
- Kazim Raza
- Kehinde Giwa
- Kyrun Philip-Lessells
- Sabrina Kaur
- Samuel Smith
- Yoonhee Lee
- Luis Rodriguez Valido
- Rajpal Aujla

### Trainers
- Luke Fairbrass
- Paula Savaglia

Thank you all for your dedication and support throughout the development of this project.


