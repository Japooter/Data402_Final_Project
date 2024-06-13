import boto3
import datetime
import json
import re
from io import StringIO

import numpy as np
import pandas as pd
import sqlalchemy as sa

s3 = boto3.client('s3')

connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=sa;'
    'PWD=Ducks123;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)


def list_objects(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents'] if obj['Key'] != prefix]
    return []


def list_all_objects(bucket, prefix):
    all_objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' in response:
            all_objects.extend([obj['Key'] for obj in response['Contents']])

        if not response.get('NextContinuationToken'):
            break

        continuation_token = response['NextContinuationToken']

    return all_objects


def load_academy_data(bucket, prefix):
    files = list_all_objects(bucket, prefix)
    data_frames = []
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(obj['Body'])
        file_name = file_key.split("/")[1].split(".")[0]
        df.insert(0, 'filename', file_name, True)
        data_frames.append(df)
    return pd.concat(data_frames, ignore_index=True)


def check_talent_json_files(bucket, prefix):
    talent_list = list()
    files = list_all_objects(bucket, prefix)
    if not files:
        print("No files found in the Talent folder.")
        return

    first_file_key = files[0]
    obj = s3.get_object(Bucket=bucket, Key=first_file_key)
    first_file_data = json.load(obj['Body'])
    expected_keys = set(first_file_data.keys())

    inconsistent_files = []
    missing_values_count = 0
    number_of_talent_files = 0
    number_of_json_files = 0
    print("Please wait...")
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            if file_key[-3:] == 'son':
                number_of_json_files += 1
                data = json.load(obj['Body'])

                if set(data.keys()) != expected_keys:
                    inconsistent_files.append(file_key)
                talent_list.append(data)

                missing_values_count += sum(1 for k, v in data.items() if v in [None, "", [], {}])
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
        number_of_talent_files += 1

    return json.dumps(talent_list)


def load_talent_csv_data(bucket, prefix):
    files = list_all_objects(bucket, prefix)
    data_frames = []
    for file_key in files:
        if file_key.split(".")[-1] == "csv":
            obj = s3.get_object(Bucket=bucket, Key=file_key)
            df = pd.read_csv(obj['Body'])
            file_name = file_key.split("/")[1].split(".")[0][:-10]
            df.insert(1, 'filename', file_name, True)
            data_frames.append(df)
        else:
            continue
    return pd.concat(data_frames, ignore_index=True)


def list_txt_files(bucket, prefix):
    all_files = list_all_objects(bucket, prefix)
    txt_files = [file for file in all_files if file.endswith('.txt')]
    return txt_files


def clean_whitespace(text):
    try:
        return text.strip()
    except:
        return text


def get_category(filename):
    splits = filename.split("_")
    category = splits[0]
    return category


def get_stream(filename):
    splits = filename.split("_")
    stream_name = "".join(splits[0:2])
    return stream_name


def get_date(filename):
    splits = filename.split("_")
    date = datetime.datetime.strptime(splits[2], "%Y-%m-%d").date()
    return date


def clean_month(month_name):
    #print(month_name)
    if month_name:
        month_name2 = month_name[:-4].strip()
        year = month_name[-4:].strip()
        month_name2 = month_name2.capitalize()
        if month_name2 == "Sept":
            month_name2 = "September"
        cleaned_month = month_name2 + '-' + year
        return cleaned_month
    else:
        return month_name


def clean_phone_numbers(phone_no):
    to_replace = ["-", " ", "(", ")"]
    try:
        for item in to_replace:
            phone_no = phone_no.replace(item, "")
        return phone_no
    except:
        return phone_no


def combine_date_and_month(invited_day, month):
    try:
        invited_date = str(int(invited_day)) + '-' + month
        return_date = datetime.datetime.strptime(invited_date, '%d-%B-%Y').date()
        return return_date
    except:
        return month


def dobs_to_datetime(date_ofb):
    try:
        strp_dob = datetime.datetime.strptime(date_ofb, '%d/%m/%Y').date()
        return strp_dob
    except:
        return date_ofb


def capitalise(string):
    try:
        string = string.title()
        return string
    except:
        return string


def get_txt_file_contents(bucket, key):
    # Retrieve the object from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    # Read the content of the file
    content = response['Body'].read().decode('utf-8')
    return content


def parse_txt_content(content):
    lines = content.strip().split('\n')
    date = lines[0]
    academy = lines[1]
    data = []
    for line in lines[2:]:
        match = re.match(r'(.+?) -\s+Psychometrics:\s+(\d+)/100,\s+Presentation:\s+(\d+)/32', line)
        if match:
            name = match.group(1).strip().title()
            psychometrics = match.group(2).strip()
            presentation = match.group(3).strip()
            data.append([date, academy, name, psychometrics, presentation])
    return data


def combine_txt_files(bucket, prefix):
    txt_files = list_txt_files(bucket, prefix)
    all_data = []
    for txt_file in txt_files:
        content = get_txt_file_contents(bucket, txt_file)
        file_data = parse_txt_content(content)
        all_data.extend(file_data)

    df = pd.DataFrame(all_data, columns=['date', 'academy', 'name', 'psychometric_score', 'presentation_score'])
    return df

def remove_wildcards(name):
    new_name = re.sub("[^A-Za-z'' -]", "", name)
    return new_name

def replace_yes_no_bool(string):
    out = None
    try:
        if string.lower() == "yes":
            out = True
        elif string.lower() == "no":
            out = False
        return out
    except:
        return None


def clean_academy_csv():
    academy_data = load_academy_data('data-402-final-project', 'Academy/')

    cols = list(academy_data.columns.values)
    for col in cols:
        academy_data[col] = academy_data[col].apply(clean_whitespace)

    academy_data['name'] = academy_data['name'].apply(capitalise)
    academy_data['category'] = academy_data['filename'].apply(get_category)
    academy_data['stream'] = academy_data['filename'].apply(get_stream)
    academy_data['date'] = academy_data['filename'].apply(get_date)
    academy_data = academy_data[['category', 'stream', 'date'] + [col for col in academy_data if
                                                                  col not in ['category', 'stream', 'date',
                                                                              'filename']]]
    return academy_data


def clean_talent_json():
    bucket_name = 'data-402-final-project'
    talent_prefix = 'Talent/'

    json_files = check_talent_json_files(bucket_name, talent_prefix)
    df = pd.read_json(StringIO(json_files))
    #df = df.fillna('{}')
    df['tech_self_score'] = df['tech_self_score'].apply(json.dumps)
    df['weaknesses'] = df['weaknesses'].apply(str)
    df['strengths'] = df['strengths'].apply(str)
    df['name'] = df['name'].apply(capitalise)
    df['self_development'] = df['self_development'].apply(replace_yes_no_bool)
    df['geo_flex'] = df['geo_flex'].apply(replace_yes_no_bool)
    df['financial_support_self'] = df['financial_support_self'].apply(replace_yes_no_bool)

    df = df.drop_duplicates()

    df['tech_self_score'] = df['tech_self_score'].replace('NaN', None)

    return df


def clean_talent_csv():
    bucket_name = 'data-402-final-project'
    talent_prefix = 'Talent/'

    talent_data = load_talent_csv_data(bucket_name, talent_prefix)

    talent_data['phone_number'] = talent_data['phone_number'].apply(clean_phone_numbers)
    talent_data['month'] = talent_data['month'].apply(lambda x: clean_month(x) if (np.all(pd.notnull(x))) else x)

    talent_data['sparta_day_date'] = talent_data.apply(lambda x: combine_date_and_month(x['invited_date'], x['month']),
                                                       axis=1)
    talent_data = talent_data.drop(['invited_date', 'month'], axis=1)
    talent_data["dob"] = talent_data["dob"].apply(dobs_to_datetime)
    talent_data['address'] = talent_data['address'].apply(capitalise)
    talent_data['name'] = talent_data['name'].apply(capitalise)

    talent_data['name'] = talent_data['name'].apply(remove_wildcards)

    for column in list(talent_data.columns):
        talent_data[column] = talent_data[column].apply(clean_whitespace)

    return talent_data


def clean_talent_txt():
    bucket_name = 'data-402-final-project'
    talent_prefix = 'Talent/'

    talent_txt_files = combine_txt_files(bucket_name, talent_prefix)

    talent_txt_files['date'] = talent_txt_files['date'].str.strip()
    talent_txt_files['academy'] = talent_txt_files['academy'].str.strip()
    talent_txt_files['name'] = talent_txt_files['name'].str.strip()
    talent_txt_files['psychometric_score'] = talent_txt_files['psychometric_score'].astype(int)
    talent_txt_files['presentation_score'] = talent_txt_files['presentation_score'].astype(int)

    talent_txt_files['name'] = talent_txt_files['name'].apply(capitalise)

    talent_txt_files['date'] = pd.to_datetime(talent_txt_files['date']).dt.date

    return talent_txt_files


def insert_into_sql(dataframe, engine, tablename):
    dataframe.to_sql(tablename, engine, schema="dbo", if_exists="replace", index=False)
    return

def create_score_data(academy_data):

    academy_data_melted = academy_data.melt(id_vars=['category', 'stream', 'date', 'name', 'trainer'],
                                            var_name='variable', value_name='Score')
    academy_data_melted['Week'] = academy_data_melted['variable'].str.extract(r'_(W\d+)$')
    academy_data_melted['Behaviour'] = academy_data_melted['variable'].str.extract(r'^(\w+)_')
    academy_data_melted = academy_data_melted.drop(columns=['variable'])

    return academy_data_melted


if __name__ == "__main__":
    # Academy csv
    print("Processing academy csv data!")
    academy_data = clean_academy_csv()
    insert_into_sql(academy_data, engine, "Academy_CSV")
    print("Successfully inserted academy csv data!")

    # Talent json
    print("Processing talent json data!")
    talent_json = clean_talent_json()
    insert_into_sql(talent_json, engine, "Talent_JSON")
    print("Successfully inserted talent json data!")

    # Talent csv
    print("Processing talent csv data!")
    talent_csv = clean_talent_csv()
    insert_into_sql(talent_csv, engine, "Talent_CSV")
    print("Successfully inserted talent csv data!")

    # Talent txt
    print("Processing talent txt data!")
    talent_txt = clean_talent_txt()
    insert_into_sql(talent_txt, engine, "Talent_TXT")
    print("Successfully inserted talent txt data!")

    # Create Score table
    academy_data_melted = create_score_data(academy_data)
    insert_into_sql(academy_data_melted, engine, "Score")
