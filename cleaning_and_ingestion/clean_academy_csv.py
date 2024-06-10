import os, json, csv, boto3, datetime
import pandas as pd



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
        df.insert(0,'filename', file_name, True)
        data_frames.append(df)
    return pd.concat(data_frames, ignore_index=True)

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

if __name__ == "__main__":
    s3 = boto3.client('s3')
    academy_data = load_academy_data('data-402-final-project', 'Academy/')
    cols = list(academy_data.columns.values)
    for col in cols:
        academy_data[col] = academy_data[col].apply(clean_whitespace)

    academy_data['Category'] = academy_data['filename'].apply(get_category)
    academy_data['Stream'] = academy_data['filename'].apply(get_stream)
    academy_data['Date'] = academy_data['filename'].apply(get_date)
    academy_data = academy_data[['Category', 'Stream', 'Date'] + [col for col in academy_data if
                                                                  col not in ['Category', 'Stream', 'Date',
                                                                              'filename']]]
