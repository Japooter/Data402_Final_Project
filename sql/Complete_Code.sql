----------------------------------------------------------------
CREATE TABLE temp_words (
    word NVARCHAR(MAX)
);
GO

CREATE FUNCTION dbo.SplitString (
     @string NVARCHAR(MAX),
     @delimiter CHAR(1)
 )
 RETURNS @output TABLE(word NVARCHAR(MAX))
 AS
 BEGIN
     DECLARE @start INT, @end INT
     SET @start = 1
     SET @end = CHARINDEX(@delimiter, @string)
 
     WHILE @start < LEN(@string) + 1
     BEGIN
         IF @end = 0
             SET @end = LEN(@string) + 1
 
         INSERT INTO @output (word)
         VALUES (SUBSTRING(@string, @start, @end - @start))
 
         SET @start = @end + 1
         SET @end = CHARINDEX(@delimiter, @string, @start)
     END
 
     RETURN
 END;
 GO

INSERT INTO temp_words (word)
SELECT LTRIM(RTRIM(word))  
FROM [Talent_JSON]
CROSS APPLY dbo.SplitString(
    REPLACE(REPLACE(REPLACE(weaknesses, '[', ''), ']', ''), '''', ''), ','
);
GO

DROP TABLE IF EXISTS weaknesses
CREATE TABLE weaknesses (
    weakness_id INT IDENTITY PRIMARY KEY,
    weakness_name NVARCHAR(255) UNIQUE
);
GO
 
INSERT INTO weaknesses (weakness_name)
SELECT DISTINCT word
FROM temp_words;
GO

-- Create Strengths table
DROP TABLE IF EXISTS temp_words;
 
CREATE TABLE temp_words (
    word NVARCHAR(MAX)
);
GO
 
INSERT INTO temp_words (word)
SELECT LTRIM(RTRIM(word))  -- Aplicar LTRIM y RTRIM para eliminar espacios adicionales
FROM [Talent_JSON]
CROSS APPLY dbo.SplitString(
    REPLACE(REPLACE(REPLACE(strengths, '[', ''), ']', ''), '''', ''), ','
);
GO
 
DROP TABLE IF EXISTS strengths
CREATE TABLE strengths (
    strength_id INT IDENTITY PRIMARY KEY,
    strength_name NVARCHAR(255) UNIQUE
);
GO
 
INSERT INTO strengths (strength_name)
SELECT DISTINCT word
FROM temp_words;
GO
 
DROP TABLE temp_words;
GO

 

-- RENAMING COLUMNS FOR USE IN JOINS
EXEC sp_rename 'dbo.Academy_CSV.name', 'ACname', 'COLUMN';
EXEC sp_rename 'dbo.Talent_JSON.name', 'TJname', 'COLUMN';
EXEC sp_rename 'dbo.Talent_TXT.name', 'TTname', 'COLUMN';
 
EXEC sp_rename 'dbo.Academy_CSV.date', 'Startdate', 'COLUMN';
EXEC sp_rename 'dbo.Talent_JSON.date', 'TJdate', 'COLUMN';
EXEC sp_rename 'dbo.Talent_TXT.date', 'TTdate', 'COLUMN';
GO

-- EXEC sp_rename [ @objname = ] 'Academy_CSV.name' , [ @newname = ] 'ACname'
--     [ , [ @objtype = ] 'COLUMN' ]
-- DROPPING TABLE IF IT ALREADY EXISTS
DROP TABLE IF EXISTS Applicants;
GO
 
-- JOINING TABLES TOGETHER TO CREATE ONE LARGE TABLE
SELECT *
INTO Applicants
FROM Talent_CSV tc
FULL OUTER JOIN Talent_JSON tj
on tc.name = tj.TJname
FULL OUTER JOIN Talent_TXT tt
on tc.name = tt.TTname AND tc.sparta_day_date = tt.TTdate
FULL OUTER JOIN Academy_CSV ac
on tc.name = ac.ACname
 
-- DROPPING UNIMPORTANT COLUMNS
ALTER TABLE Applicants
DROP COLUMN TJdate
 
ALTER TABLE Applicants
DROP COLUMN TTdate
 
ALTER TABLE Applicants
DROP COLUMN id
 
ALTER TABLE Applicants
DROP COLUMN filename
 
ALTER TABLE Applicants
DROP COLUMN ACname
 
ALTER TABLE Applicants
DROP COLUMN TTname
 
ALTER TABLE Applicants
DROP COLUMN TJname
 
ALTER TABLE Applicants
ADD Applicant_ID INT IDENTITY(1,1) PRIMARY KEY;
GO

DROP TABLE IF EXISTS applicant_weakness_junction
 
 CREATE TABLE applicant_weakness_junction (
    weakness_id INT,
    Applicant_ID INT,
    CONSTRAINT weakness_applicant_pk PRIMARY KEY (weakness_id, Applicant_ID),
    CONSTRAINT FK_weakness FOREIGN KEY (weakness_id) REFERENCES weaknesses (weakness_id),
    CONSTRAINT FK_applicant FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID)
);
 
INSERT INTO applicant_weakness_junction (weakness_id, Applicant_ID)
SELECT w.weakness_id, a.Applicant_ID
FROM Applicants a
CROSS APPLY dbo.SplitString(
    REPLACE(REPLACE(REPLACE(a.weaknesses, '[', ''), ']', ''), '''', ''), ',' -- Eliminar los corchetes y comillas y luego dividir por comas
) s
JOIN weaknesses w ON LTRIM(RTRIM(s.word)) = w.weakness_name;

 
DROP TABLE IF EXISTS applicant_strengths_junction;
 
DROP TABLE IF EXISTS applicant_weakness_junction
 
 CREATE TABLE applicant_weakness_junction (
    weakness_id INT,
    Applicant_ID INT,
    CONSTRAINT weakness_applicant_pk PRIMARY KEY (weakness_id, Applicant_ID),
    CONSTRAINT FK_weakness FOREIGN KEY (weakness_id) REFERENCES weaknesses (weakness_id),
    CONSTRAINT FK_applicant FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID)
);
 
INSERT INTO applicant_weakness_junction (weakness_id, Applicant_ID)
SELECT w.weakness_id, a.Applicant_ID
FROM Applicants a
CROSS APPLY dbo.SplitString(
    REPLACE(REPLACE(REPLACE(a.weaknesses, '[', ''), ']', ''), '''', ''), ',' -- Eliminar los corchetes y comillas y luego dividir por comas
) s
JOIN weaknesses w ON LTRIM(RTRIM(s.word)) = w.weakness_name;
 
 
DROP TABLE IF EXISTS applicant_strengths_junction;
 
CREATE TABLE applicant_strengths_junction (
    strength_id INT,
    Applicant_ID INT,
    CONSTRAINT strength_applicant_pk PRIMARY KEY (strength_id, Applicant_ID),
    CONSTRAINT FK_strength FOREIGN KEY (strength_id) REFERENCES strengths (strength_id),
    CONSTRAINT FK_applicant_strength FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID)
);
 
INSERT INTO applicant_strengths_junction (strength_id, Applicant_ID)
SELECT s.strength_id, a.Applicant_ID
FROM Applicants a
CROSS APPLY dbo.SplitString(
    REPLACE(REPLACE(REPLACE(a.strengths, '[', ''), ']', ''), '''', ''), ',' -- Eliminar los corchetes y comillas y luego dividir por comas
) str
JOIN strengths s ON LTRIM(RTRIM(str.word)) = s.strength_name;

---------------------------------------------------------
DROP TABLE IF EXISTS Sparta_Day
DROP TABLE IF EXISTS Location
 
-- Create Sparta_Day table:
SELECT DISTINCT sparta_day_date AS "Date", academy AS 'Location'
INTO Sparta_Day
FROM Applicants
WHERE sparta_day_date IS NOT NULL;


ALTER TABLE Sparta_Day
ADD Sparta_Day_ID INT IDENTITY(1,1) PRIMARY KEY
 
-- Create Location table:
SELECT DISTINCT Location_ID = IDENTITY(INT, 1, 1), Location
INTO Location
FROM Sparta_Day
WHERE Location IS NOT NULL;
 
-- Add PK to locationID:
ALTER TABLE Location
ADD CONSTRAINT Location_ID PRIMARY KEY (Location_ID);

-- Add LocationID column:
ALTER TABLE Sparta_Day ADD Location_ID INT;
-- replace the Location name with its corresponding LocationID:
UPDATE Sparta_Day
SET Location_ID = (SELECT Location_ID FROM Location WHERE Location.Location = Sparta_Day.Location);
 
-- Remove Location column from Sparta_Day table:
ALTER TABLE Sparta_Day DROP COLUMN Location;
 
-- Add a foreign key to the LocationID column in Sparta_Day table:
ALTER TABLE Sparta_Day
ADD CONSTRAINT fk_Location_ID
FOREIGN KEY (Location_ID) REFERENCES Location(Location_ID);


ALTER TABLE Applicants ADD Sparta_Day_ID INT;
GO
-- replace the Location name with its corresponding LocationID:

UPDATE Applicants
SET Sparta_Day_ID = (SELECT MIN(Sparta_Day_ID) FROM Sparta_Day WHERE Sparta_Day.Date = Applicants.sparta_day_date);


-- SELECT * FROM Sparta_Day
-- ALTER TABLE Applicants
-- DROP COLUMN sparta_day_date
 
-- ALTER TABLE Applicants
-- DROP COLUMN academy
---------------------------------------------------

DROP TABLE IF EXISTS Address;
DROP TABLE IF EXISTS Talent_Coordinators;
DROP TABLE IF EXISTS Uni;


-- ########### Address Table #############
-- SELECT * FROM Address;

CREATE TABLE Address (
    Address_ID INT IDENTITY(1,1) PRIMARY KEY,
    Address_Name VARCHAR(255) NOT NULL,
    City_Name VARCHAR(255) NOT NULL,
    Postcode_Name VARCHAR(100) NOT NULL
);
GO

INSERT INTO Address (Address_Name, City_Name, Postcode_Name) 
SELECT DISTINCT address, city, postcode FROM Talent_CSV 
WHERE address IS NOT NULL AND city IS NOT NULL AND postcode IS NOT NULL;

ALTER TABLE Applicants ADD Address_ID INT;
ALTER TABLE Applicants
ADD CONSTRAINT fk_AddressID
FOREIGN KEY (Address_ID) REFERENCES Address(Address_ID);
GO

UPDATE Applicants
SET Address_ID = (SELECT Address_ID FROM Address WHERE Applicants.city = Address.City_Name AND Applicants.address = Address.Address_Name);
GO
-- ############ CREATE UNI TABLE ##############
-- SELECT * FROM Applicants;
-- SELECT * FROM Uni;
DROP TABLE IF EXISTS Uni;

CREATE TABLE Uni (
    Uni_ID INT IDENTITY(1,1) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

ALTER TABLE Applicants ADD Uni_ID INT;

INSERT INTO Uni (Name) 
SELECT DISTINCT uni FROM Talent_CSV 
WHERE uni IS NOT NULL;

ALTER TABLE Applicants
ADD CONSTRAINT fk_UniID
FOREIGN KEY (Uni_ID) REFERENCES Uni(Uni_ID);
GO

UPDATE Applicants
SET Uni_ID = (SELECT Uni_ID FROM Uni WHERE Applicants.uni = Uni.Name);
GO

-- ##### CREATE TALENT_COORDINATORS TABLE ############
-- Fifi Etton and Fifi Eton could be the same person? With following query below, you can see how many times they appear.
-- SELECT tc.name, COUNT(a.name) FROM Talent_Coordinators tc INNER JOIN Applicants a ON tc.Name = a.invited_by GROUP BY tc.name ORDER BY COUNT(a.name) ASC;

CREATE TABLE Talent_Coordinators (
    Talent_Coordinator_ID INT IDENTITY(1,1) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL);

ALTER TABLE Applicants ADD Talent_Coordinator_ID INT;

INSERT INTO Talent_Coordinators (Name) 
SELECT DISTINCT invited_by
FROM Talent_CSV
WHERE invited_by IS NOT NULL;

ALTER TABLE Applicants
ADD CONSTRAINT fk_Talent_CoordinatorID
FOREIGN KEY (Talent_Coordinator_ID) REFERENCES Talent_Coordinators(Talent_Coordinator_ID);
GO

UPDATE Applicants
SET Talent_Coordinator_ID = (SELECT Talent_Coordinator_ID FROM Talent_Coordinators WHERE Applicants.invited_by = Talent_Coordinators.Name);
GO
-- ########### Address Table

-----------------------------------------------------------------------------------------------------------------------------------------
--############# CREATING STREAMS TABLE #########

-- Create STREAMS table:
SELECT DISTINCT stream AS "Stream", category AS 'Category', trainer AS 'Trainer_Name'
INTO Streams
FROM Applicants
WHERE stream IS NOT NULL;

--############# CREATING STREAM JUNCTION TABLE #########

-- Create STREAMS JUNCTION table:
SELECT Applicant_ID AS "Applicant_ID"
INTO Streams_Junction
FROM Applicants
WHERE Applicant_ID IS NOT NULL;

ALTER TABLE Streams_Junction DROP COLUMN IF EXISTS Stream;
ALTER TABLE Streams_Junction ADD Stream VARCHAR(25);

-- set the stream name first to be converted to ID value
UPDATE Streams_Junction
SET Stream = (SELECT stream FROM Applicants WHERE Applicants.Applicant_ID = Streams_Junction.Applicant_ID AND Applicants.stream IS NOT NULL);
 
DELETE FROM Streams_Junction WHERE Stream IS NULL;

--######## ADDING START_DATE TO STREAMS ###################

ALTER TABLE Streams DROP COLUMN IF EXISTS Start_Date;
ALTER TABLE Streams ADD Start_Date DATE;
ALTER TABLE Streams_Junction DROP COLUMN IF EXISTS Start_Date;

UPDATE Streams
SET Start_Date = (SELECT MIN(sparta_day_date) FROM Applicants
WHERE Applicants.stream = Streams.Stream AND Applicants.stream IS NOT NULL);

--############### ADDING CATEGORY TABLE ############
ALTER TABLE Streams DROP COLUMN IF EXISTS Start_Date;
ALTER TABLE Streams ADD Start_Date DATE;

SELECT DISTINCT Category_ID = IDENTITY(INT, 1, 1), Category
INTO Categories
FROM Streams
WHERE Category IS NOT NULL;

UPDATE Streams
SET Category = (SELECT Category_ID FROM Categories
WHERE Categories.Category = Streams.Category);

exec sp_rename 'Streams.Category',  'Category_ID', 'COLUMN'; 

ALTER TABLE Categories
ALTER COLUMN Category_ID INT NOT NULL;

ALTER TABLE Categories
ADD CONSTRAINT Category_ID PRIMARY KEY (Category_ID);

--########## CREATING TRAINERS TABLE ###########

SELECT DISTINCT Trainer_ID = IDENTITY(INT, 1, 1), Trainer_Name, Stream
INTO Trainers
FROM Streams
WHERE Trainer_Name IS NOT NULL;

ALTER TABLE Trainers
ADD CONSTRAINT Trainer_ID PRIMARY KEY (Trainer_ID);

--###### CREATING TRAINERS JUNCTION #############

SELECT Stream
INTO Trainer_Junction
FROM Streams
WHERE Stream IS NOT NULL;

ALTER TABLE Streams ADD Trainer_ID INT;

UPDATE Streams
SET Trainer_ID = (SELECT MIN(Trainer_ID) FROM Trainers WHERE Trainers.Trainer_Name = Streams.Trainer_Name)

ALTER TABLE Trainer_Junction ADD Trainer_ID INT;

UPDATE Trainer_Junction
SET Trainer_ID = (SELECT Trainer_ID FROM Streams WHERE Streams.Stream = Trainer_Junction.Stream);

--######### SETTING UP REMAINING FOREIGN AND PRIMARY KEYS

ALTER TABLE Trainer_Junction
ALTER COLUMN Trainer_ID INT NOT NULL;

ALTER TABLE Trainer_Junction
ALTER COLUMN Stream VARCHAR(100) NOT NULL;

ALTER TABLE Trainers
ALTER COLUMN Stream VARCHAR(100) NOT NULL;

ALTER TABLE Trainer_Junction
ADD CONSTRAINT Trainer_Stream_PK PRIMARY KEY (Stream, Trainer_ID)

ALTER TABLE Streams
ALTER COLUMN Stream VARCHAR(100) NOT NULL;

ALTER TABLE Streams
ADD CONSTRAINT Stream_ID PRIMARY KEY (Stream);

ALTER TABLE Streams DROP COLUMN Trainer_Name;

exec sp_rename 'Streams_Junction.Stream',  'Stream_ID', 'COLUMN';
exec sp_rename 'Streams.Stream', 'Stream_ID', 'COLUMN';
exec sp_rename 'Trainer_Junction.Stream', 'Stream_ID', 'COLUMN';

ALTER TABLE Streams_Junction
ALTER COLUMN Stream_ID VARCHAR(100) NOT NULL;

ALTER TABLE Streams_Junction
ALTER COLUMN Applicant_ID INT NOT NULL;

ALTER TABLE Streams_Junction
ALTER COLUMN Stream_ID VARCHAR(100) NOT NULL;

ALTER TABLE Streams_Junction
ADD CONSTRAINT Stream_Applicant_PK PRIMARY KEY (Stream_ID, Applicant_ID);

ALTER TABLE Trainer_Junction
ADD CONSTRAINT fk_TrainerID
FOREIGN KEY (Trainer_ID) REFERENCES Trainers(Trainer_ID);
ALTER TABLE Trainer_Junction
ADD CONSTRAINT fk_StreamID
FOREIGN KEY (Stream_ID) REFERENCES Streams(Stream_ID);
ALTER TABLE Streams
ALTER COLUMN Category_ID INT NOT NULL;
ALTER TABLE Streams
ADD CONSTRAINT fk_CategoryID
FOREIGN KEY (Category_ID) REFERENCES Categories(Category_ID);

ALTER TABLE Streams_Junction
ADD CONSTRAINT fk_ApplicantID
FOREIGN KEY (Applicant_ID) REFERENCES Applicants(Applicant_ID);

ALTER TABLE Streams_Junction
ADD CONSTRAINT fk_StreamID_j
FOREIGN KEY (Stream_ID) REFERENCES Streams(Stream_ID);

---------------------------------------------------------------------------------------------------------
--Drop tables if they already exist

DROP TABLE IF EXISTS Tech_Self_Score, #Tech_Self_Score, #Tech_Self_Score_unpvt, Applicant_Tech_jct;
GO
DROP TABLE IF EXISTS Tech_Skill
GO
-- Create the table and insert values as portrayed in the previous example. 
 SELECT 
    Applicant_ID,
    JSON_VALUE(tech_self_score, '$."C#"') AS CSharp,
    JSON_VALUE(tech_self_score, '$."Java"') AS Java,
    JSON_VALUE(tech_self_score, '$."R"') AS R,
    JSON_VALUE(tech_self_score, '$."JavaScript"') AS JavaScript,
    JSON_VALUE(tech_self_score, '$.Python') AS Python,
    JSON_VALUE(tech_self_score, '$."C++"') AS CPlusPlus,
    JSON_VALUE(tech_self_score, '$.Ruby') AS Ruby,
    JSON_VALUE(tech_self_score, '$."SPSS"') AS SPSS,
    JSON_VALUE(tech_self_score, '$."PHP"') AS PHP
INTO #Tech_Self_Score
FROM Applicants
WHERE tech_self_score IS NOT NULL;

-- Drop tech_self_score from applicants table
ALTER TABLE Applicants DROP COLUMN tech_self_score;

-- Unpivot the table.  
SELECT Applicant_ID, Tech, Score 
INTO #Tech_Self_Score_unpvt
FROM   
   (SELECT Applicant_ID, CSharp, Java, R, JavaScript, Python, CPlusPlus, Ruby, SPSS, PHP 
   FROM #Tech_Self_Score) p  
UNPIVOT  
   (Score FOR Tech IN   
      (CSharp, Java, R, JavaScript, Python, CPlusPlus, Ruby, SPSS, PHP)  
)AS tech;  

-- Create tech skill table
CREATE TABLE Tech_Skill (
    Tech_ID int IDENTITY(1,1) PRIMARY KEY,
    Tech_Name VARCHAR(255) NOT NULL
);

-- Create tech skill temp table 
INSERT INTO Tech_Skill
SELECT DISTINCT (Tech)
FROM #Tech_Self_Score_unpvt;

-- Create applicant/tech junction table
CREATE TABLE Applicant_Tech_jct (
   Applicant_ID int NOT NULL,
   Tech_ID int  NOT NULL,
   Score int NOT NULL,
)

ALTER TABLE Applicant_Tech_jct
ADD CONSTRAINT fk_applicant_tech FOREIGN KEY (Applicant_ID) REFERENCES Applicants(Applicant_ID);

ALTER TABLE Applicant_Tech_jct
ADD CONSTRAINT fk_tech FOREIGN KEY (Tech_ID) REFERENCES Tech_Skill(Tech_ID);

-- Insert values into applicant/tech junction table
INSERT INTO Applicant_Tech_jct
Select Applicant_ID, Tech_ID, Score
FROM Tech_Skill
JOIN #Tech_Self_Score_unpvt
ON #Tech_Self_Score_unpvt.Tech = Tech_Skill.Tech_Name
ORDER BY Applicant_ID;


-- Assign primary/foreign keys
ALTER TABLE Applicant_Tech_jct
ADD CONSTRAINT pk_Applicant_Tech_ID PRIMARY KEY (Applicant_ID, Tech_ID);

-- Drop temp tables
DROP TABLE #Tech_Self_Score, #Tech_Self_Score_unpvt;
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS Score_Junction
DROP TABLE IF EXISTS Behaviour
GO
 
SELECT name, behaviour, week, score
INTO Score_Junction
FROM Score
WHERE name IS NOT NULL;
-- Add Applicant_ID column to Score_Junction table:
ALTER TABLE Score_Junction ADD Applicant_ID INT;
GO
-- replace the name with its corresponding Applicant_ID:
UPDATE Score_Junction
SET Applicant_ID = (SELECT Applicant_ID FROM Applicants WHERE Applicants.name = Score_JUnction.name);
 
-- Remove name column from Score_Junction table:
ALTER TABLE Score_Junction DROP COLUMN name;
 
-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN Applicant_ID INT NOT NULL;
 
-- Add a foreign key to the Applicant_ID column in Score table:
ALTER TABLE Score_Junction
ADD CONSTRAINT fk_Applicant_ID
FOREIGN KEY (Applicant_ID) REFERENCES Applicants(Applicant_ID);
 
-- Create Behaviour table
SELECT DISTINCT Behaviour_ID = IDENTITY(INT, 1, 1), Behaviour
INTO Behaviour
FROM Score_Junction
WHERE Behaviour IS NOT NULL;
GO
 
-- Add PK to Behaviour_ID:
ALTER TABLE Behaviour
ADD CONSTRAINT Behaviour_ID PRIMARY KEY (Behaviour_ID);
 
-- Add Behaviour_ID column to Score_Junction table:
ALTER TABLE Score_Junction ADD Behaviour_ID INT;
GO
-- replace the Behaviour with its corresponding Behaviour_ID:
UPDATE Score_Junction
SET Behaviour_ID = (SELECT Behaviour_ID FROM Behaviour WHERE Behaviour.Behaviour = Score_Junction.Behaviour);
GO
 
-- Remove Behaviour column from Score_Junction table:
ALTER TABLE Score_Junction DROP COLUMN Behaviour;
 
-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN Behaviour_ID INT NOT NULL;
 
-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN week INT NOT NULL;
 
-- Add a foreign key to the Behaviour_ID column in Score_Junction table:
ALTER TABLE Score_Junction
ADD CONSTRAINT fk_Behaviour_ID
FOREIGN KEY (Behaviour_ID) REFERENCES Behaviour(Behaviour_ID);
GO
-- change foreign keys to composite key
ALTER TABLE Score_Junction
ADD CONSTRAINT pk_Score_Junction_ID PRIMARY KEY (Applicant_ID, Behaviour_ID, week);

---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TempTable
  
SELECT
    Applicant_ID,
    name AS "Applicant_Name",
    gender AS "Gender",
    dob AS "DOB",
    email AS "Email",
    phone_number AS "Phone_Number",
    Address_ID,
    Uni_ID,
    degree AS "Degree",
    Sparta_Day_ID,
    self_development AS "Self_Development",
    geo_flex AS "Geo_Flex",
    financial_support_self AS "Financial_Support_Self",
    course_interest AS "Course_Interest",
    psychometric_score AS "Psychometric_Score",
    presentation_Score AS "Presentation_Score",
    result AS "Result",
    Talent_Coordinator_ID
INTO TempTable
FROM Applicants

EXEC sp_rename 'Applicants', 'Dropped'
EXEC sp_rename 'TempTable', 'Applicants'

ALTER TABLE Applicants
ADD CONSTRAINT Applicant_ID PRIMARY KEY (Applicant_ID)

ALTER TABLE Applicants
ADD FOREIGN KEY (Address_ID) REFERENCES Address(Address_ID)

ALTER TABLE Applicants
ADD FOREIGN KEY (Sparta_Day_ID) REFERENCES Sparta_Day(Sparta_Day_ID)

ALTER TABLE Applicants
ADD FOREIGN KEY (UNI_ID) REFERENCES Uni(UNI_ID)

ALTER TABLE Applicants
ADD FOREIGN KEY (Talent_Coordinator_ID) REFERENCES Talent_Coordinators(Talent_Coordinator_ID)

ALTER TABLE applicant_weakness_junction
DROP CONSTRAINT FK_applicant;

ALTER TABLE applicant_weakness_junction
ADD CONSTRAINT FK_applicant_weakness FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID);

ALTER TABLE applicant_strengths_junction
DROP CONSTRAINT FK_applicant_strength;

ALTER TABLE applicant_strengths_junction
ADD CONSTRAINT FK_applicant_strength FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID);

ALTER TABLE Streams_Junction
DROP CONSTRAINT fk_ApplicantID;

ALTER TABLE Streams_Junction
ADD CONSTRAINT FK_applicant_stream FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID);

ALTER TABLE Applicant_Tech_jct
DROP CONSTRAINT fk_applicant_tech;

ALTER TABLE Applicant_Tech_jct
ADD CONSTRAINT FK_applicant_tech FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID);

ALTER TABLE Score_Junction
DROP CONSTRAINT fk_Applicant_ID;

ALTER TABLE Score_Junction
ADD CONSTRAINT FK_applicant_score FOREIGN KEY (Applicant_ID) REFERENCES Applicants (Applicant_ID);
GO



DROP TABLE Talent_CSV
DROP TABLE Talent_JSON
DROP TABLE Talent_TXT
DROP TABLE Academy_CSV
DROP TABLE Score
DROP TABLE Dropped
