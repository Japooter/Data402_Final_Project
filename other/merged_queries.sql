/*
Run each block in order (1, then 2, then 3, etc). Uncomment and run each block sequentially.
After running, comment out the block to keep track of progress.
*/

/* 1. Drop existing constraints */
/*
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_APPLICANTS_ADDRESS_ID')
    ALTER TABLE APPLICANTS DROP CONSTRAINT FK_APPLICANTS_ADDRESS_ID;

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_APPLICANTS_UNI_ID')
    ALTER TABLE APPLICANTS DROP CONSTRAINT FK_APPLICANTS_UNI_ID;

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_APPLICANTS_TALENT_COORDINATOR_ID')
    ALTER TABLE APPLICANTS DROP CONSTRAINT FK_APPLICANTS_TALENT_COORDINATOR_ID;
*/

/* 2. Drop existing tables if they exist */
/*
DROP TABLE IF EXISTS APPLICANTS;
DROP TABLE IF EXISTS ADDRESS;
DROP TABLE IF EXISTS UNI;
DROP TABLE IF EXISTS TALENT_COORDINATORS;
*/

/* 3. Create necessary tables */
/*
CREATE TABLE ADDRESS (
    ADDRESS_ID INT IDENTITY(1,1) PRIMARY KEY,
    ADDRESS_NAME VARCHAR(255) NOT NULL
);

CREATE TABLE UNI (
    UNI_ID INT IDENTITY(1,1) PRIMARY KEY,
    NAME VARCHAR(255) NOT NULL
);

CREATE TABLE TALENT_COORDINATORS (
    TALENT_COORDINATOR_ID INT IDENTITY(1,1) PRIMARY KEY,
    NAME VARCHAR(255) NOT NULL
);

CREATE TABLE APPLICANTS (
    APPLICANT_ID INT IDENTITY(1,1) PRIMARY KEY,
    NAME VARCHAR(255) NOT NULL,
    DOB DATE NOT NULL,
    EMAIL VARCHAR(255) NOT NULL,
    ADDRESS_ID INT,
    UNI_ID INT,
    DEGREE_GRADE VARCHAR(50),
    TALENT_COORDINATOR_ID INT,
    PHONE_NUMBER VARCHAR(255),
    FOREIGN KEY (ADDRESS_ID) REFERENCES ADDRESS(ADDRESS_ID),
    FOREIGN KEY (UNI_ID) REFERENCES UNI(UNI_ID),
    FOREIGN KEY (TALENT_COORDINATOR_ID) REFERENCES TALENT_COORDINATORS(TALENT_COORDINATOR_ID)
);
*/

/* 4. Update and insert data from Talent_CSV */
/*
UPDATE Talent_CSV 
SET address = 'Address Is Unknown' 
WHERE address IS NULL;

INSERT INTO ADDRESS (ADDRESS_NAME) 
SELECT DISTINCT address 
FROM Talent_CSV;

UPDATE Talent_CSV 
SET uni = 'University Is Unknown' 
WHERE uni IS NULL;

INSERT INTO UNI (NAME) 
SELECT DISTINCT uni 
FROM Talent_CSV;

UPDATE Talent_CSV 
SET invited_by = 'Talent Coordinator Unknown' 
WHERE invited_by IS NULL;

INSERT INTO TALENT_COORDINATORS (NAME) 
SELECT DISTINCT invited_by
FROM Talent_CSV
WHERE invited_by NOT IN (SELECT NAME FROM TALENT_COORDINATORS);

INSERT INTO APPLICANTS (NAME, DOB, EMAIL, ADDRESS_ID, UNI_ID, DEGREE_GRADE, TALENT_COORDINATOR_ID, PHONE_NUMBER)
SELECT 
    t.name, 
    ISNULL(t.dob, '1900-01-01') AS DOB,
    ISNULL(t.email, 'Unknown') AS EMAIL,
    a.ADDRESS_ID, 
    u.UNI_ID, 
    t.degree, 
    tc.TALENT_COORDINATOR_ID,
    t.phone_number 
FROM 
    Talent_CSV t
JOIN 
    ADDRESS a ON t.address = a.ADDRESS_NAME
JOIN 
    UNI u ON t.uni = u.NAME
JOIN 
    TALENT_COORDINATORS tc ON t.invited_by = tc.NAME;
*/

/* FINAL: Populate strengths and weaknesses data */

/* Part 1: Clear existing data and extract unique strengths */
/*
DELETE FROM STRENGTHS_JUNCTION;
DELETE FROM STRENGTHS;

CREATE TABLE #TEMP_STRENGTHS (
  name VARCHAR(255) PRIMARY KEY
);

INSERT INTO #TEMP_STRENGTHS (name)
SELECT DISTINCT TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS strength_name
FROM Talent_JSON
CROSS APPLY STRING_SPLIT(strengths, ',');

INSERT INTO STRENGTHS (name)
SELECT name
FROM #TEMP_STRENGTHS;

DROP TABLE #TEMP_STRENGTHS;
*/

/* Part 2: Populate the STRENGTHS_JUNCTION table */
/*
CREATE TABLE #TEMP_APPLICANT_STRENGTHS (
  applicant_id INT,
  strength_name VARCHAR(255)
);

INSERT INTO #TEMP_APPLICANT_STRENGTHS (applicant_id, strength_name)
SELECT a.applicant_id, TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS strength_name
FROM Talent_JSON tj
JOIN APPLICANTS a ON tj.name = a.name
CROSS APPLY STRING_SPLIT(tj.strengths, ',');

INSERT INTO STRENGTHS_JUNCTION (applicant_id, strength_id)
SELECT DISTINCT tas.applicant_id, s.strength_id
FROM #TEMP_APPLICANT_STRENGTHS tas
JOIN STRENGTHS s ON tas.strength_name = s.name
LEFT JOIN STRENGTHS_JUNCTION sj ON tas.applicant_id = sj.applicant_id AND s.strength_id = sj.strength_id
WHERE sj.applicant_id IS NULL AND sj.strength_id IS NULL;

DROP TABLE #TEMP_APPLICANT_STRENGTHS;
*/

/* Create and populate weaknesses tables */
/*
CREATE TABLE WEAKNESSES (
  weakness_id INT IDENTITY(1,1) PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

CREATE TABLE WEAKNESSES_JUNCTION (
  applicant_id INT,
  weakness_id INT,
  FOREIGN KEY (applicant_id) REFERENCES APPLICANTS(applicant_id),
  FOREIGN KEY (weakness_id) REFERENCES WEAKNESSES(weakness_id),
  PRIMARY KEY (applicant_id, weakness_id)
);

DELETE FROM WEAKNESSES_JUNCTION;
DELETE FROM WEAKNESSES;

CREATE TABLE #TEMP_WEAKNESSES (
  name VARCHAR(255) PRIMARY KEY
);

INSERT INTO #TEMP_WEAKNESSES (name)
SELECT DISTINCT TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS weakness_name
FROM Talent_JSON
CROSS APPLY STRING_SPLIT(weaknesses, ',');

INSERT INTO WEAKNESSES (name)
SELECT name
FROM #TEMP_WEAKNESSES;

DROP TABLE #TEMP_WEAKNESSES;

CREATE TABLE #TEMP_APPLICANT_WEAKNESSES (
  applicant_id INT,
  weakness_name VARCHAR(255)
);

INSERT INTO #TEMP_APPLICANT_WEAKNESSES (applicant_id, weakness_name)
SELECT a.applicant_id, TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS weakness_name
FROM Talent_JSON tj
JOIN APPLICANTS a ON tj.name = a.name
CROSS APPLY STRING_SPLIT(tj.weaknesses, ',');

INSERT INTO WEAKNESSES_JUNCTION (applicant_id, weakness_id)
SELECT DISTINCT taw.applicant_id, w.weakness_id
FROM #TEMP_APPLICANT_WEAKNESSES taw
JOIN WEAKNESSES w ON taw.weakness_name = w.name
LEFT JOIN WEAKNESSES_JUNCTION wj ON taw.applicant_id = wj.applicant_id AND w.weakness_id = wj.weakness_id
WHERE wj.applicant_id IS NULL AND wj.weakness_id IS NULL;

DROP TABLE #TEMP_APPLICANT_WEAKNESSES;
*/

/* Retrieve applicant strengths and weaknesses */
/*
SELECT 
    a.name AS ApplicantName, 
    w.name AS Weakness, 
    s.name AS Strength
FROM 
    APPLICANTS a
LEFT JOIN 
    WEAKNESSES_JUNCTION wj ON a.applicant_id = wj.applicant_id
LEFT JOIN 
    WEAKNESSES w ON wj.weakness_id = w.weakness_id
LEFT JOIN 
    STRENGTHS_JUNCTION sj ON a.applicant_id = sj.applicant_id
LEFT JOIN 
    STRENGTHS s ON sj.strength_id = s.strength_id;
*/

/* Add new columns to APPLICANTS and update data */

/*
ALTER TABLE APPLICANTS ADD psychometric_score INT;
UPDATE APPLICANTS 
SET APPLICANTS.psychometric_score = t.psychometric_score
FROM APPLICANTS a
INNER JOIN Talent_TXT t ON a.name = t.name
WHERE t.psychometric_score IS NOT NULL;

ALTER TABLE APPLICANTS ADD presentation_score INT;
UPDATE APPLICANTS 
SET APPLICANTS.presentation_score = t.presentation_score
FROM APPLICANTS a
INNER JOIN Talent_TXT t ON a.name = t.name
WHERE t.presentation_score IS NOT NULL;

ALTER TABLE APPLICANTS ADD gender VARCHAR(255);
UPDATE APPLICANTS
SET APPLICANTS.gender = t.gender
FROM APPLICANTS a
INNER JOIN Talent_CSV t ON a.name = t.name
WHERE t.gender IS NOT NULL;

ALTER TABLE APPLICANTS ADD course_interest VARCHAR(255);
UPDATE APPLICANTS
SET APPLICANTS.course_interest = j.course_interest
FROM APPLICANTS a
INNER JOIN Talent_JSON j ON a.name = j.name
WHERE j.course_interest IS NOT NULL;

ALTER TABLE APPLICANTS ADD financial_support_self BIT;
UPDATE app
SET app.financial_support_self = 
  CASE 
    WHEN j.financial_support_self = 'Yes' THEN 1
    WHEN j.financial_support_self = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;

ALTER TABLE APPLICANTS ADD geo_flex BIT;
UPDATE app
SET app.geo_flex = 
  CASE 
    WHEN j.geo_flex = 'Yes' THEN 1
    WHEN j.geo_flex = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;

ALTER TABLE APPLICANTS ADD self_development BIT;
UPDATE app
SET app.self_development = 
  CASE 
    WHEN j.self_development = 'Yes' THEN 1
    WHEN j.self_development = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;

ALTER TABLE APPLICANTS ADD result VARCHAR(255);
UPDATE APPLICANTS
SET APPLICANTS.result = t.result
FROM APPLICANTS a
INNER JOIN Talent_JSON t ON a.name = t.name
WHERE t.result IS NOT NULL;
