-- RENAMING COLUMNS FOR USE IN JOINS
EXEC sp_rename 'dbo.Academy_CSV.name', 'ACname', 'COLUMN';
EXEC sp_rename 'dbo.Talent_JSON.name', 'TJname', 'COLUMN';
EXEC sp_rename 'dbo.Talent_TXT.name', 'TTname', 'COLUMN';
 
EXEC sp_rename 'dbo.Academy_CSV.ACdate', 'Startdate', 'COLUMN';
EXEC sp_rename 'dbo.Talent_JSON.date', 'TJdate', 'COLUMN';
EXEC sp_rename 'dbo.Talent_TXT.date', 'TTdate', 'COLUMN';
 
-- DROPPING TABLE IF IT ALREADY EXISTS
DROP TABLE IF EXISTS Applicants
 
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
ADD Applicant_ID INT IDENTITY(1,1) PRIMARY KEY