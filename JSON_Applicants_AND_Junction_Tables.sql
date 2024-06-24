DROP TABLE IF EXISTS temp_words;
 
CREATE TABLE temp_words (
    word NVARCHAR(MAX)
);
GO

-- CREATE FUNCTION dbo.SplitString (
--     @string NVARCHAR(MAX),
--     @delimiter CHAR(1)
-- )
-- RETURNS @output TABLE(word NVARCHAR(MAX))
-- AS
-- BEGIN
--     DECLARE @start INT, @end INT
--     SET @start = 1
--     SET @end = CHARINDEX(@delimiter, @string)
 
--     WHILE @start < LEN(@string) + 1
--     BEGIN
--         IF @end = 0
--             SET @end = LEN(@string) + 1
 
--         INSERT INTO @output (word)
--         VALUES (SUBSTRING(@string, @start, @end - @start))
 
--         SET @start = @end + 1
--         SET @end = CHARINDEX(@delimiter, @string, @start)
--     END
 
--     RETURN
-- END;
-- GO

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

 
-- SELECT * FROM weaknesses ORDER BY weakness_id;
-- SELECT * FROM strengths ORDER BY strength_id;

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
 
SELECT * FROM applicant_weakness_junction
SELECT Applicant_id, strengths FROM dbo.Applicants
 
 
SELECT a.name, a.Applicant_id, w.weakness_name 
FROM dbo.Applicants a 
INNER JOIN applicant_weakness_junction aw 
ON a.Applicant_id = aw.Applicant_ID
INNER JOIN dbo.weaknesses w 
ON aw.weakness_id = w.weakness_id
 
 
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
 
SELECT * FROM applicant_strengths_junction
SELECT name, Applicant_id, strengths FROM dbo.Applicants
 
 
SELECT a.name, a.Applicant_id, s.strength_name 
FROM dbo.Applicants a 
INNER JOIN applicant_strengths_junction asj 
ON a.Applicant_id = asj.Applicant_ID
INNER JOIN dbo.strengths s 
ON asj.strength_id = s.strength_id