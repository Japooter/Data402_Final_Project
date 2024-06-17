############# CREATING STREAMS TABLE #########

-- Create STREAMS table:
SELECT DISTINCT stream AS "Stream", category AS 'Category', trainer AS 'Trainer_Name'
INTO Streams
FROM Applicants
WHERE stream IS NOT NULL;

############# CREATING STREAM JUNCTION TABLE #########

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

######## ADDING START_DATE TO STREAMS ###################

ALTER TABLE Streams DROP COLUMN IF EXISTS Start_Date;
ALTER TABLE Streams ADD Start_Date DATE;
ALTER TABLE Streams_Junction DROP COLUMN IF EXISTS Start_Date;

UPDATE Streams
SET Start_Date = (SELECT MIN(date) FROM Applicants
WHERE Applicants.stream = Streams.Stream AND Applicants.stream IS NOT NULL);

############### ADDING CATEGORY TABLE ############
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
ALTER COLUMN Category_ID VARCHAR(100) NOT NULL;

ALTER TABLE Categories
ADD CONSTRAINT Category_ID PRIMARY KEY (Category_ID);

########## CREATING TRAINERS TABLE ###########

SELECT DISTINCT Trainer_ID = IDENTITY(INT, 1, 1), Trainer_Name
INTO Trainers
FROM Streams
WHERE Trainer_Name IS NOT NULL;

ALTER TABLE Trainers
ADD CONSTRAINT Trainer_ID PRIMARY KEY (Trainer_ID);

###### CREATING TRAINERS JUNCTION #############

SELECT Stream
INTO Trainer_Junction
FROM Streams
WHERE Stream IS NOT NULL;

ALTER TABLE Streams ADD Trainer_ID INT;

UPDATE Streams
SET Trainer_ID = (SELECT Trainer_ID FROM Trainers WHERE Trainers.Trainer_Name = Streams.Trainer_Name)

ALTER TABLE Trainer_Junction ADD Trainer_ID INT;

UPDATE Trainer_Junction
SET Trainer_ID = (SELECT Trainer_ID FROM Streams WHERE Streams.Stream = Trainer_Junction.Stream);

######### SETTING UP REMAINING FOREIGN AND PRIMARY KEYS

ALTER TABLE Trainer_Junction
ALTER COLUMN Trainer_ID INT NOT NULL;

ALTER TABLE Trainers
ALTER COLUMN Stream VARCHAR(100) NOT NULL;

ALTER TABLE Trainer_Junction
ADD CONSTRAINT Trainer_Stream_PK PRIMARY KEY (Stream, Trainer_ID)

ALTER TABLE Streams
ALTER COLUMN Stream VARCHAR(100) NOT NULL;

ALTER TABLE Streams
ADD CONSTRAINT Stream_ID PRIMARY KEY (Stream);

ALTER TABLE Streams DROP COLUMN Trainer_Name;

ALTER TABLE Trainers DROP COLUMN Streams;

ALTER TABLE Streams_Junction DROP COLUMN Start_Date;

exec sp_rename 'Streams_Junction.Stream',  'Stream_ID', 'COLUMN';
exec sp_rename 'Streams.Stream', 'Stream_ID', 'COLUMN';
exec sp_rename 'Trainer_Junction.Stream', 'Stream_ID', 'COLUMN';

ALTER TABLE Streams_Junction
ALTER COLUMN Stream_ID VARCHAR(100) NOT NULL;

ALTER TABLE Streams_Junction
ALTER COLUMN Applicant_ID INT NOT NULL;

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
