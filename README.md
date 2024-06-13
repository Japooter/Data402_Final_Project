# Data 402 Final Project
The repository for the work produced on the project by Data group 402. <br>

## Project overview: 

Task: Create an ETL pipeline from mock data from a fictionalised version of Sparta global and investigate possible patterns. <br>

Requirements: <br>

The code needs to be production-level <br>

An appropriate datastore that will house all the data drawn from the files, the database should provide a single person view <br>


## ETL process: 

### Extraction 

The Sparta global mock data was extracted from the Amazon S3 bucket 'data-402-final-project bucket which contained two further prefixes Talent and academy. <br>

Talent contents: Data collected on the Sparta day event, contains information about candidate competency, the outcome of behaviour and their performance and various recruitment tests. <br>

.json <br>

.csv <br>

.txt <br> 

Academy contents: Data from all the Spartans, with their name, course and what score they are getting in their weekly tests. <br>

.csv <br>

 

### Transformation 

#### Data cleaning: 

The final project bucket contained a ‘Talent’ prefix and an “Academy’ prefix. <br>

Within the 'Talent' prefix, we had over 3000 JSON files that include information on potential talent for Sparta Global. There were also (number) CSV files, and (number) TXT files. The ‘Academy’ prefix contained (number) CSV files. <br>

As the data consisted of three file types, JSON, CSV, and TXT, we sought out to implement cleaning and pre-processing for each file type. We separated into three teams to handle each respective file type. The CSV team separated further, to account for the respective prefixes, ‘Talent’ and ‘Academy’. <br>

 

**JSON files:** 

To begin the cleaning process, we first ensured that the files could all be brought together into one file. As there was a 1000 file limit  (forgot the context ), we had to alter the code to account for this. We extracted all 3105 JSON files into a single JSON file via python, labelled 'talent.json'. <br> 

We used this file to create a data-frame using Pandas. Using Pandas enabled us to explore and clean the data thoroughly. Through exploration of the data, we noted that 55 of the 3105 files did not contain information for the column 'tech_self_score'. While these in their individual files would have been simply missing, when placed in the data frame, these 55 rows contained NaN under this column. To handle this missing data, we deliberated over dropping the rows, considering its relatively small volume. However, we decided to simply convert the NaN values within 'tech_self_score' into a string resembling an empty dictionary, '{}'. We also converted the columns ‘strengths’, and ‘weaknesses’ into strings. <br>

This decision was made with consideration of the dropping of data that we were about to perform next.  As the columns ‘tech_self_score', ‘strengths’, and ‘weaknesses’ contained dictionaries and lists, our search for the duplicated values would not prove to be successful as Pandas cannot deduce what is within lists and dictionaries when searching for duplicate rows. The choice to convert those three columns into strings alleviate this issue. <br>

Once everything had become a string, the duplicated rows could then be found. In searching for duplicates, we found 32 rows indicated as such. Once we confirmed that the rows returned were truly duplicated, we dropped the duplicates using the command ‘drop_duplicates()’, allowing us to have a finalised data frame with still over 3000 rows, containing no NaN values, and no duplicates. <br>

 

**CSV files:** 

 

**TXT files:** 
