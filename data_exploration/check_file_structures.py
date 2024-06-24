import boto3
import pandas as pd
import json

s3 = boto3.client('s3')

bucket_name = 'data-402-final-project'
academy_prefix = 'Academy/'
talent_prefix = 'Talent/'

def list_objects(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents'] if obj['Key'] != prefix]
    return []

def get_total_records(bucket, prefix):
    files = list_objects(bucket, prefix)
    total_records = 0
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            if file_key.endswith('.csv'):
                df = pd.read_csv(obj['Body'])
                total_records += len(df)
            elif file_key.endswith('.json'):
                data = json.load(obj['Body'])
                total_records += 1
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
    return total_records

def check_academy_files(bucket, prefix):
    files = list_objects(bucket, prefix)
    if not files:
        print("No files found in the Academy folder.")
        return
    
    inconsistent_files = []
    missing_values_count = 0
    academy_names = set()
   
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            df = pd.read_csv(obj['Body'])
            academy_names.update(df['name'].tolist())
            if df.columns.tolist() != ['name', 'trainer', 'Analytic_W1', 'Independent_W1', 'Determined_W1']:
                inconsistent_files.append(file_key)
            missing_values_count += df.isnull().sum().sum()
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
    
    print("Academy Files Summary:")
    print(f"Number of inconsistent files: {len(inconsistent_files)}")
    print(f"Total number of missing values: {missing_values_count}")
    print(f"Total number of records: {get_total_records(bucket, prefix)}")
    return academy_names

def check_talent_files(bucket, prefix, academy_names):
    files = list_objects(bucket, prefix)
    if not files:
        print("No files found in the Talent folder.")
        return
    
    inconsistent_files = []
    missing_values_count = 0
    talent_names = set()
    
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            data = json.load(obj['Body'])
            talent_names.add(data['name'])
            if set(data.keys()) != {'name', 'date', 'tech_self_score', 'strengths', 'weaknesses', 'self_development'}:
                inconsistent_files.append(file_key)
            missing_values_count += sum(1 for k, v in data.items() if v in [None, "", [], {}])
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
    
    print("Talent Files Summary:")
    print(f"Number of inconsistent files: {len(inconsistent_files)}")
    print(f"Total number of missing values: {missing_values_count}")
    print(f"Total number of records: {get_total_records(bucket, prefix)}")
    
    # if inconsistent_files:
    #     print("Files with inconsistencies:")
    #     for file in inconsistent_files:
    #         print(file)
    
    # Check the number of people in Academy but not in Talent
    num_missing_names = len(academy_names - talent_names)
    print(f"\nNumber of people in Academy but not in Talent: {num_missing_names}")


print("Checking Academy files:")
academy_names = check_academy_files(bucket_name, academy_prefix)

print("\nChecking Talent files:")
check_talent_files(bucket_name, talent_prefix, academy_names)
