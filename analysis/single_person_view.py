import boto3
import datetime
import json
import re
from io import StringIO
import pickle
import numpy as np
import pandas
import pandas as pd
import sqlalchemy
import sqlalchemy as sa

s3 = boto3.client('s3')

connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=FinalProject;'
    'UID=SA;'
    'PWD=Ext3rm1n@t3;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)


smaller_query = '''
SELECT
    Applicant_ID,
    Applicant_Name,
    Behaviour,
    [Behaviour Score],
    Week
FROM
(
    SELECT
        a.Applicant_ID,
        a.Applicant_Name,
        a.DOB,
        addr.Address_Name,
        addr.City_Name,
        addr.Postcode_Name,
        u.Name AS University_Name,
        a.Degree AS University_Degree_Result,
        a.Result AS Sparta_Academy_Result,
        s.Date AS Sparta_Day_Date,
        l.Location AS Sparta_Day_Location,
        a.Presentation_Score,
        a.Psychometric_Score,
        asj.strength_id,
        str.strength_name,
        weak.weakness_name,
        a.Course_Interest,
        sj.Stream_ID,
        train.Trainer_Name,
        awj.weakness_id,
        ts.Tech_Name,
        tsj.Score,
        scoj.[week] AS 'Week',
        scoj.score AS 'Behaviour Score',
        scoj.Behaviour_ID AS 'Behaviour ID',
        beha.Behaviour,
        tcoord.Name AS 'Talent_Coordinator_Name'
    FROM 
        Applicants a
    LEFT JOIN 
        Address addr ON a.Address_ID = addr.Address_ID
    LEFT JOIN 
        Uni u ON a.Uni_ID = u.Uni_ID
    LEFT JOIN 
        Applicant_Tech_jct tsj ON a.Applicant_ID = tsj.Applicant_ID
    LEFT JOIN 
        Tech_Skill ts ON tsj.Tech_ID = ts.Tech_ID
    LEFT JOIN 
        Sparta_Day s ON a.Sparta_Day_ID = s.Sparta_Day_ID
    LEFT JOIN 
        [Location] l ON s.Location_ID = l.Location_ID
    LEFT JOIN 
        applicant_strengths_junction asj ON a.Applicant_ID = asj.Applicant_ID
    LEFT JOIN 
        strengths str ON asj.strength_id = str.strength_id
    LEFT JOIN
        applicant_weakness_junction awj ON a.Applicant_ID = awj.Applicant_ID
    LEFT JOIN 
        weaknesses weak ON awj.weakness_id = weak.weakness_id
    LEFT JOIN 
        Streams_Junction sj ON a.Applicant_ID = sj.Applicant_ID
    LEFT JOIN 
        Streams streams ON sj.Stream_ID = streams.Stream_ID
    LEFT JOIN 
        Trainers train ON streams.Trainer_ID = train.Trainer_ID
    LEFT JOIN 
        Score_Junction scoj ON a.Applicant_ID = scoj.Applicant_ID
    LEFT JOIN
        Behaviour beha ON scoj.Behaviour_ID = beha.Behaviour_ID
    LEFT JOIN
        Talent_Coordinators tcoord ON a.Talent_Coordinator_ID = tcoord.Talent_Coordinator_ID
) AS SourceTable
PIVOT
(
    MAX(Score)
    FOR Tech_Name IN ([PHP], [CPlusPlus], [Java], [JavaScript], [Python], [Ruby], [SPSS], [R])
) AS PivotTechScores    
PIVOT
(
    MAX(strength_name)
    FOR strength_id IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25])
) AS PivotStrengths
PIVOT 
( 
    COUNT (weakness_ID)
    FOR weakness_name IN ([Anxious],
[Chaotic],
[Chatty],
[Competitive],
[Controlling],
[Conventional],
[Critical],
[Distracted],
[Immature],
[Impatient],
[Impulsive],
[Indecisive],
[Indifferent],
[Intolerant],
[Introverted],
[Overbearing],
[Passive],
[Perfectionist],
[Procrastination],
[Selfish],
[Sensitive],
[Slow],
[Stubborn],
[Undisciplined])
) AS PivotTableWeakness
ORDER BY 
    Applicant_Name, [Week];'''

query = '''
SELECT
    Applicant_ID,
    Applicant_Name,
    DOB,
    Address_Name AS 'Address Name',
    City_Name AS 'City',
    Postcode_Name AS 'Postcode',
    University_Name AS 'University Name',
    University_Degree_Result AS 'University Degree Result',
    Geo_Flex AS 'Geo Flexible',
    Financial_Support_Self AS 'Self-Financial Support',
    Self_Development AS 'Self Development',
    [PHP] AS 'PHP Score',
    [CPlusPlus] AS 'C++ Score',
    [Java] AS 'Java Score',
    [JavaScript] AS 'JavaScript Score',
    [Python] AS 'Python Score',
    [Ruby] AS 'Ruby Score',
    [SPSS] AS 'SPSS Score',
    [R] AS 'R Score',
    Course_Interest AS 'Course Interest',
    Sparta_Academy_Result AS 'Interview Result',
    Sparta_Day_Date AS 'Sparta Day Date',
    Sparta_Day_Location AS 'Sparta Day Location',
    Presentation_Score AS 'Presentation Score',
    Psychometric_Score AS 'Psychometric Score',
    [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25],
    [Anxious],
    [Chaotic],
    [Chatty],
    [Competitive],
    [Controlling],
    [Conventional],
    [Critical],
    [Distracted],
    [Immature],
    [Impatient],
    [Impulsive],
    [Indecisive],
    [Indifferent],
    [Intolerant],
    [Introverted],
    [Overbearing],
    [Passive],
    [Perfectionist],
    [Procrastination],
    [Selfish],
    [Sensitive],
    [Slow],
    [Stubborn],
    [Undisciplined],
    Stream_ID AS 'Stream Name',
    Start_Date AS 'Stream Start Date',
    Trainer_Name AS 'Trainer Name',
    Behaviour,
    [Behaviour Score],
    Week,
    Talent_Coordinator_Name AS 'Talent Coordinator Name'
FROM
(
    SELECT
        a.Applicant_ID,
        a.Applicant_Name,
        a.DOB,
        a.Self_Development,
        a.Geo_Flex,
        a.Financial_Support_Self,
        addr.Address_Name,
        addr.City_Name,
        addr.Postcode_Name,
        u.Name AS University_Name,
        a.Degree AS University_Degree_Result,
        a.Result AS Sparta_Academy_Result,
        s.Date AS Sparta_Day_Date,
        l.Location AS Sparta_Day_Location,
        a.Presentation_Score,
        a.Psychometric_Score,
        asj.strength_id,
        str.strength_name,
        weak.weakness_name,
        a.Course_Interest,
        sj.Stream_ID,
        streams.Start_Date,
        train.Trainer_Name,
        awj.weakness_id,
        ts.Tech_Name,
        tsj.Score,
        scoj.[week] AS 'Week',
        scoj.score AS 'Behaviour Score',
        scoj.Behaviour_ID AS 'Behaviour ID',
        beha.Behaviour,
        tcoord.Name AS 'Talent_Coordinator_Name'
    FROM 
        Applicants a
    LEFT JOIN 
        Address addr ON a.Address_ID = addr.Address_ID
    LEFT JOIN 
        Uni u ON a.Uni_ID = u.Uni_ID
    LEFT JOIN 
        Applicant_Tech_jct tsj ON a.Applicant_ID = tsj.Applicant_ID
    LEFT JOIN 
        Tech_Skill ts ON tsj.Tech_ID = ts.Tech_ID
    LEFT JOIN 
        Sparta_Day s ON a.Sparta_Day_ID = s.Sparta_Day_ID
    LEFT JOIN 
        [Location] l ON s.Location_ID = l.Location_ID
    LEFT JOIN 
        applicant_strengths_junction asj ON a.Applicant_ID = asj.Applicant_ID
    LEFT JOIN 
        strengths str ON asj.strength_id = str.strength_id
    LEFT JOIN
        applicant_weakness_junction awj ON a.Applicant_ID = awj.Applicant_ID
    LEFT JOIN 
        weaknesses weak ON awj.weakness_id = weak.weakness_id
    LEFT JOIN 
        Streams_Junction sj ON a.Applicant_ID = sj.Applicant_ID
    LEFT JOIN 
        Streams streams ON sj.Stream_ID = streams.Stream_ID
    LEFT JOIN 
        Trainers train ON streams.Trainer_ID = train.Trainer_ID
    LEFT JOIN 
        Score_Junction scoj ON a.Applicant_ID = scoj.Applicant_ID
    LEFT JOIN
        Behaviour beha ON scoj.Behaviour_ID = beha.Behaviour_ID
    LEFT JOIN
        Talent_Coordinators tcoord ON a.Talent_Coordinator_ID = tcoord.Talent_Coordinator_ID
) AS SourceTable
PIVOT
(
    MAX(Score)
    FOR Tech_Name IN ([PHP], [CPlusPlus], [Java], [JavaScript], [Python], [Ruby], [SPSS], [R])
) AS PivotTechScores    
PIVOT
(
    MAX(strength_name)
    FOR strength_id IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25])
) AS PivotStrengths
PIVOT 
( 
    COUNT (weakness_ID)
    FOR weakness_name IN ([Anxious],
[Chaotic],
[Chatty],
[Competitive],
[Controlling],
[Conventional],
[Critical],
[Distracted],
[Immature],
[Impatient],
[Impulsive],
[Indecisive],
[Indifferent],
[Intolerant],
[Introverted],
[Overbearing],
[Passive],
[Perfectionist],
[Procrastination],
[Selfish],
[Sensitive],
[Slow],
[Stubborn],
[Undisciplined])
) AS PivotTableWeakness
ORDER BY 
    Applicant_ID, [Week];
'''

## UNCOMMENT BELOW ON FIRST RUN OF SCRIPT

# Load data into Pandas dataframe
df = pd.read_sql_query(smaller_query, engine)
bigDF = pd.read_sql_query(query, engine)
with open('../sql_query_df_shorter.pkl', 'wb') as f:
    pickle.dump(df, f)
with open('../sql_query_df.pkl', 'wb') as f:
    pickle.dump(bigDF, f)

### LOAD THE PKL FILES WITH BELOW

with open('../sql_query_df_shorter.pkl', 'rb') as f:
    df = pickle.load(f)
with open('../sql_query_df.pkl', 'rb') as f:
    bigDF = pickle.load(f)
print(df)


# First we need to sort out the behaviour values. The best way to do this would be to pivot the dataframe into separate Week 1-10
# columns with a dictionary in each. In each week's dictionary, the keys would be the behaviours and the values would be the score.

# The smaller query above is grouped based on the following three columns as the combination would always be a unique variable.
grouped = df.groupby(['Applicant_ID', 'Applicant_Name', 'Week'])

# The lambda function below transforms the values in both the Behaviour and Behaviour Score columns into a dictionary, with the behaviour name as the key and the behaviour score as the value.
pivot_df = grouped.apply(lambda x: dict(sorted(zip(x['Behaviour'], x['Behaviour Score']))))

pivot_df = pivot_df.reset_index()

# Pivot the dataframe to spread the 10 Week values for each student into separate columns
pivot_df = pivot_df.pivot(index=['Applicant_ID', 'Applicant_Name'], columns='Week').fillna({})

# Changes the names of the non-index columns in the pivoted dataframe to 'Week <int>'
pivot_df.columns = [f'Week {round(col[1])}' for col in pivot_df.columns]
pivot_df = pivot_df.reset_index()

columns_to_drop = ['Behaviour', 'Behaviour Score', 'Week']
bigDF.drop(columns=columns_to_drop, inplace=True)

bigDF = pd.merge(right=pivot_df, left=bigDF, how='left', on=['Applicant_ID', 'Applicant_Name'])

# Display the final dataframe
#print(pivot_df)

bigDF_behaviours = bigDF.drop_duplicates(subset=['Applicant_ID'])
#
# # Display the updated dataframe
# print("\nDataFrame after keeping only the first occurrence of duplicates:")
bigDFSorted = bigDF_behaviours.sort_values(by='Applicant_ID')
#print(bigDFSorted)
pd.set_option('display.max_columns', None)
#print(bigDFSorted[bigDFSorted['Applicant_Name'] == 'Aida Bothams'])

### Strengths (convert to list inside Strengths column)

# The strengths are held across columns with names of numbers 1-25 (this is because of how it has been pivoted)
columns_to_process = [str(i) for i in range(1, 26)]

# This processes which columns have names as their values then adds these strengths to a list for each row
strengths_column = bigDFSorted.apply(
    lambda row: {row[col] for col in columns_to_process if row[col] is not None},
    axis=1
)

# This adds the column Strengths to the dataframe and drops the old integer columns below
bigDFSorted.insert(25, 'Strengths', strengths_column)

columns_to_drop = [str(i) for i in range(1, 26)]
bigDFSorted.drop(columns=columns_to_drop, inplace=True)
# print(bigDFSorted[bigDFSorted['Applicant_Name'] == 'Aida Bothams'])

#### Weaknesses column (insert into new column 'Weaknesses' and convert to a list)

# The weaknesses are held as boolean values based on if the row has the column name as a weakness. This might be confusing but it's the
# only way I could pivot both sets of information to Python to then transform here.

columns_to_process2 = ['Anxious', 'Chaotic', 'Chatty', 'Competitive', 'Controlling', 'Conventional', 'Critical',
                       'Distracted',
                       'Immature', 'Impatient', 'Impulsive', 'Indecisive', 'Indifferent',
                       'Intolerant', 'Introverted', 'Overbearing', 'Passive', 'Perfectionist',
                       'Procrastination', 'Selfish', 'Sensitive', 'Slow', 'Stubborn', 'Undisciplined']

# This adds the column name to a list based on if the boolean value is True. This list is added to a new column of lists Weaknesses.
weaknesses_column = bigDFSorted.apply(lambda row: {col for col in columns_to_process2 if row[col] != 0}, axis=1)
bigDFSorted.insert(26, 'Weaknesses', weaknesses_column)

columns_to_drop2 = ['Anxious', 'Chaotic', 'Chatty', 'Competitive', 'Controlling', 'Conventional', 'Critical',
                    'Distracted',
                    'Immature', 'Impatient', 'Impulsive', 'Indecisive', 'Indifferent',
                    'Intolerant', 'Introverted', 'Overbearing', 'Passive', 'Perfectionist',
                    'Procrastination', 'Selfish', 'Sensitive', 'Slow', 'Stubborn', 'Undisciplined']
bigDFSorted.drop(columns=columns_to_drop2, inplace=True)

print(bigDFSorted[bigDFSorted['Applicant_Name'] == 'Anet Satterley'])

bigDFSorted['Strengths'] = bigDFSorted['Strengths'].apply(lambda x: ', '.join(x) if isinstance(x, set) else x)
bigDFSorted['Weaknesses'] = bigDFSorted['Weaknesses'].apply(lambda x: ', '.join(x) if isinstance(x, set) else x)

# Ensuring the Week Columns are Json strings with 'dumps'

week_columns = [f'Week {i}' for i in range(1, 11)]
for col in week_columns:
    bigDFSorted[col] = bigDFSorted[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

print(bigDFSorted[bigDFSorted['Applicant_Name'] == 'Anet Satterley'])

bigDFSorted.to_sql('SinglePersonView', engine, schema="dbo", if_exists="replace", index=False)
