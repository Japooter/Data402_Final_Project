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

def check_academy_files(bucket, prefix):
    files = list_objects(bucket, prefix)
    if not files:
        print("No files found in the Academy folder.")
        return
    

    first_file_key = files[0]
    obj = s3.get_object(Bucket=bucket, Key=first_file_key)
    first_file_df = pd.read_csv(obj['Body'])
    expected_columns = first_file_df.columns.tolist()
    

    inconsistent_files = []
    missing_values_count = 0
    
   
    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            df = pd.read_csv(obj['Body'])
            if df.columns.tolist() != expected_columns:
                inconsistent_files.append(file_key)
          
            missing_values_count += df.isnull().sum().sum()
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
    

    print("Academy Files Summary:")
    print(f"Number of inconsistent files: {len(inconsistent_files)}")
    print(f"Total number of missing values: {missing_values_count}")

def check_talent_files(bucket, prefix):
    files = list_objects(bucket, prefix)
    if not files:
        print("No files found in the Talent folder.")
        return
    
 
    first_file_key = files[0]
    obj = s3.get_object(Bucket=bucket, Key=first_file_key)
    first_file_data = json.load(obj['Body'])
    expected_keys = set(first_file_data.keys())
    

    inconsistent_files = []
    missing_values_count = 0
    

    for file_key in files:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        try:
            data = json.load(obj['Body'])
            if set(data.keys()) != expected_keys:
                inconsistent_files.append(file_key)

            missing_values_count += sum(1 for k, v in data.items() if v in [None, "", [], {}])
        except Exception as e:
            print(f"Error reading file {file_key}: {e}")
    

    print("Talent Files Summary:")
    print(f"Number of inconsistent files: {len(inconsistent_files)}")
    print(f"Total number of missing values: {missing_values_count}")
    
    if inconsistent_files:
        print("Files with inconsistencies:")
        for file in inconsistent_files:
            print(file)


print("Checking Academy files:")
check_academy_files(bucket_name, academy_prefix)

print("Checking Talent files:")
check_talent_files(bucket_name, talent_prefix)


