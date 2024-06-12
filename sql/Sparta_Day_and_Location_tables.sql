DROP TABLE IF EXISTS Sparta_Day
DROP TABLE IF EXISTS Location
 
-- Create Sparta_Day table:
SELECT DISTINCT sparta_day_date AS "Date", academy AS 'Location'
INTO Sparta_Day
FROM applicants
WHERE Date IS NOT NULL;
 
ALTER TABLE Sparta_Day
ADD Sparta_DayID INT IDENTITY(1,1) PRIMARY KEY
 
-- Create Location table:
SELECT DISTINCT LocationID = IDENTITY(INT, 1, 1), Location
INTO Location
FROM Sparta_Day
WHERE Location IS NOT NULL;
 
-- Add PK to locationID:
ALTER TABLE Location
ADD CONSTRAINT Location_ID PRIMARY KEY (LocationID);
 
-- Add LocationID column:
ALTER TABLE Sparta_Day ADD LocationID INT;
-- replace the Location name with its corresponding LocationID:
UPDATE Sparta_Day
SET LocationID = (SELECT LocationID FROM Location WHERE Location.Location = Sparta_Day.Location);
 
-- Remove Location column from Sparta_Day table:
ALTER TABLE Sparta_Day DROP COLUMN Location;
 
-- Add a foreign key to the LocationID column in Sparta_Day table:
ALTER TABLE Sparta_Day
ADD CONSTRAINT fk_LocationID
FOREIGN KEY (LocationID) REFERENCES Location(LocationID);


ALTER TABLE Applicants ADD Sparta_DayID INT;
-- replace the Location name with its corresponding LocationID:
UPDATE Applicants
SET Sparta_DayID = (SELECT Sparta_DayID FROM Sparta_Day WHERE Sparta_Day.Date = Applicants.sparta_day_date);