/*  Run each block in order (1, then 2, then 3 etc), comment out each block after you run the code
(so only the block you are running is not commented out) */


/*               */

/*              

DROP TABLE IF EXISTS WEAKNESSES;


DROP TABLE IF EXISTS STRENGTHS;

DROP TABLE IF EXISTS STRENGTH_JUNCTION;

DROP TABLE IF EXISTS WEAKNESS_JUNCTION;

*/

/*  
CREATE TABLE STRENGTHS (
  strength_id INT IDENTITY(1,1) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  -- You can add additional columns for strength descriptions, categories, etc.
);


CREATE TABLE STRENGTHS_JUNCTION (
  applicant_id INT,
  strength_id INT,
  FOREIGN KEY (applicant_id) REFERENCES APPLICANTS(applicant_id),
  FOREIGN KEY (strength_id) REFERENCES STRENGTHS(strength_id),
  PRIMARY KEY (applicant_id, strength_id)  -- Composite Primary Key
);
*/

/*               */


/*             
SELECT * FROM APPLICANTS;

SELECT * FROM STRENGTHS;

SELECT * FROM STRENGTHS_JUNCTION;
*/

/*       ................................................................................................        */

/*              
-- Clear any existing data in the STRENGTHS_JUNCTION table
DELETE FROM STRENGTHS_JUNCTION;
 */

/*               
-- Part 1: Extract unique strengths and insert them into the STRENGTHS table

-- First, create a temporary table to hold the unique strengths
CREATE TABLE #TEMP_STRENGTHS (
  name VARCHAR(255) PRIMARY KEY
);

-- Insert unique strengths into the temporary table
INSERT INTO #TEMP_STRENGTHS (name)
SELECT DISTINCT TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS strength_name
FROM Talent_JSON
CROSS APPLY STRING_SPLIT(strengths, ',');

-- Insert the unique strengths from the temporary table into the STRENGTHS table
INSERT INTO STRENGTHS (name)
SELECT name
FROM #TEMP_STRENGTHS;

-- Drop the temporary table as it is no longer needed
DROP TABLE #TEMP_STRENGTHS;
*/

/*     ...............2...............          */

/*
-- Clear any existing data in the STRENGTHS_JUNCTION table
DELETE FROM STRENGTHS_JUNCTION;

-- First, create a temporary table to hold the applicant and strength mappings
CREATE TABLE #TEMP_APPLICANT_STRENGTHS (
  applicant_id INT,
  strength_name VARCHAR(255)
);

-- Insert applicant and strength mappings into the temporary table
INSERT INTO #TEMP_APPLICANT_STRENGTHS (applicant_id, strength_name)
SELECT a.applicant_id, TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS strength_name
FROM Talent_JSON tj
JOIN APPLICANTS a ON tj.name = a.name  -- Using 'name' as the unique identifier
CROSS APPLY STRING_SPLIT(tj.strengths, ',');

-- Insert the mappings from the temporary table into the STRENGTHS_JUNCTION table, avoiding duplicates
INSERT INTO STRENGTHS_JUNCTION (applicant_id, strength_id)
SELECT DISTINCT tas.applicant_id, s.strength_id
FROM #TEMP_APPLICANT_STRENGTHS tas
JOIN STRENGTHS s ON tas.strength_name = s.name
LEFT JOIN STRENGTHS_JUNCTION sj ON tas.applicant_id = sj.applicant_id AND s.strength_id = sj.strength_id
WHERE sj.applicant_id IS NULL AND sj.strength_id IS NULL;

-- Drop the temporary table as it is no longer needed
DROP TABLE #TEMP_APPLICANT_STRENGTHS;
               */



/*     ...............FINAL.................................................................          */
/*     
-- Clear existing data in the STRENGTHS_JUNCTION table
DELETE FROM STRENGTHS_JUNCTION;

-- Clear existing data in the STRENGTHS table
DELETE FROM STRENGTHS;

-- Part 1: Extract unique strengths and insert them into the STRENGTHS table
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

-- Part 2: Populate the STRENGTHS_JUNCTION table
-- First, create a temporary table to hold the applicant and strength mappings
CREATE TABLE #TEMP_APPLICANT_STRENGTHS (
  applicant_id INT,
  strength_name VARCHAR(255)
);

INSERT INTO #TEMP_APPLICANT_STRENGTHS (applicant_id, strength_name)
SELECT a.applicant_id, TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS strength_name
FROM Talent_JSON tj
JOIN APPLICANTS a ON tj.name = a.name
CROSS APPLY STRING_SPLIT(tj.strengths, ',');

-- Insert the mappings from the temporary table into the STRENGTHS_JUNCTION table, avoiding duplicates
INSERT INTO STRENGTHS_JUNCTION (applicant_id, strength_id)
SELECT DISTINCT tas.applicant_id, s.strength_id
FROM #TEMP_APPLICANT_STRENGTHS tas
JOIN STRENGTHS s ON tas.strength_name = s.name
LEFT JOIN STRENGTHS_JUNCTION sj ON tas.applicant_id = sj.applicant_id AND s.strength_id = sj.strength_id
WHERE sj.applicant_id IS NULL AND sj.strength_id IS NULL;

-- Drop the temporary table as it is no longer needed
DROP TABLE #TEMP_APPLICANT_STRENGTHS;
*/

/*        */
/*         */
/*          */

/*        

CREATE TABLE WEAKNESSES (
  weakness_id INT IDENTITY(1,1) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  -- You can add additional columns for strength descriptions, categories, etc.
);


CREATE TABLE WEAKNESSES_JUNCTION (
  applicant_id INT,
  weakness_id INT,
  FOREIGN KEY (applicant_id) REFERENCES APPLICANTS(applicant_id),
  FOREIGN KEY (weakness_id) REFERENCES WEAKNESSES(weakness_id),
  PRIMARY KEY (applicant_id, weakness_id)  -- Composite Primary Key
);
*/

/*         
-- Clear existing data in the WEAKNESSES_JUNCTION table
DELETE FROM WEAKNESSES_JUNCTION;

-- Clear existing data in the WEAKNESSES table
DELETE FROM WEAKNESSES;

-- Part 1: Extract unique weaknesses and insert them into the WEAKNESSES table
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

-- Part 2: Populate the WEAKNESSES_JUNCTION table
-- First, create a temporary table to hold the applicant and weakness mappings
CREATE TABLE #TEMP_APPLICANT_WEAKNESSES (
  applicant_id INT,
  weakness_name VARCHAR(255)
);

INSERT INTO #TEMP_APPLICANT_WEAKNESSES (applicant_id, weakness_name)
SELECT a.applicant_id, TRIM(REPLACE(REPLACE(REPLACE(value, '[', ''), ']', ''), '"', '')) AS weakness_name
FROM Talent_JSON tj
JOIN APPLICANTS a ON tj.name = a.name
CROSS APPLY STRING_SPLIT(tj.weaknesses, ',');

-- Insert the mappings from the temporary table into the WEAKNESSES_JUNCTION table, avoiding duplicates
INSERT INTO WEAKNESSES_JUNCTION (applicant_id, weakness_id)
SELECT DISTINCT taw.applicant_id, w.weakness_id
FROM #TEMP_APPLICANT_WEAKNESSES taw
JOIN WEAKNESSES w ON taw.weakness_name = w.name
LEFT JOIN WEAKNESSES_JUNCTION wj ON taw.applicant_id = wj.applicant_id AND w.weakness_id = wj.weakness_id
WHERE wj.applicant_id IS NULL AND wj.weakness_id IS NULL;

-- Drop the temporary table as it is no longer needed
DROP TABLE #TEMP_APPLICANT_WEAKNESSES;
 */

/*        */
/*         */


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

/*         */

/*
ALTER TABLE APPLICANTS ADD psychometric_score int;
*/


/*         */
/* 
UPDATE APPLICANTS 
SET APPLICANTS.psychometric_score = t.psychometric_score
FROM APPLICANTS a
INNER JOIN Talent_TXT t ON a.name = t.name  -
WHERE t.psychometric_score IS NOT NULL;
*/

/*         
ALTER TABLE APPLICANTS ADD presentation_score int;
*/

/*         
UPDATE APPLICANTS 
SET APPLICANTS.presentation_score = t.presentation_score
FROM APPLICANTS a
INNER JOIN Talent_TXT t ON a.name = t.name  
WHERE t.presentation_score IS NOT NULL;
*/

/*         
ALTER TABLE APPLICANTS ADD gender VARCHAR(255);
*/

/*         */
/*         */
/*         */
/* 
UPDATE APPLICANTS
SET APPLICANTS.gender = t.gender
FROM APPLICANTS a
INNER JOIN Talent_CSV t ON a.name = t.name  
WHERE t.gender IS NOT NULL;
*/

/*         */
/*         */
/*         */
/*  
ALTER TABLE APPLICANTS ADD course_interest VARCHAR(255);
 */

/*        
UPDATE APPLICANTS
SET APPLICANTS.course_interest = j.course_interest
FROM APPLICANTS a
INNER JOIN Talent_JSON j ON a.name = j.name  
WHERE j.course_interest IS NOT NULL;
 */

/*         
 ALTER TABLE APPLICANTS ADD financial_support_self BIT;
*/

/*         */
/*         */
/*         
UPDATE app
SET app.financial_support_self = 
  CASE 
    WHEN j.financial_support_self = 'Yes' THEN 1
    WHEN j.financial_support_self = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;
*/

/*        
ALTER TABLE APPLICANTS ADD geo_flex BIT;
 */


/*         
UPDATE app
SET app.geo_flex = 
  CASE 
    WHEN j.geo_flex = 'Yes' THEN 1
    WHEN j.geo_flex = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;
*/

/*         */
/*         
ALTER TABLE APPLICANTS ADD self_development BIT;
*/

/*         
UPDATE app
SET app.self_development = 
  CASE 
    WHEN j.self_development = 'Yes' THEN 1
    WHEN j.self_development = 'No' THEN 0
    ELSE NULL
  END
FROM APPLICANTS app
INNER JOIN Talent_JSON j ON app.name = j.name;
*/

/*         
ALTER TABLE APPLICANTS ADD result VARCHAR(255);
*/

/*         */
/*         
UPDATE APPLICANTS
SET APPLICANTS.result = t.result
FROM APPLICANTS a
INNER JOIN Talent_JSON t ON a.name = t.name  -- Assuming name is used for matching
WHERE t.result IS NOT NULL;
*/

/*        
ALTER TABLE APPLICANTS ADD sparta_day_id INT;
 */


/*         
CREATE TABLE LOCATION (
  location_id INT PRIMARY KEY IDENTITY(1,1),  -- Identity for unique IDs
  location VARCHAR(255) NOT NULL  -- Location name
);

CREATE TABLE SPARTA_DAY (
  sparta_day_id INT PRIMARY KEY IDENTITY(1,1),
  date DATE NOT NULL,  -- Date of the Sparta Day
  location_id INT NOT NULL,
  FOREIGN KEY (location_id) REFERENCES LOCATION(location_id)  -- Foreign key referencing LOCATION table
);
*/



SELECT * FROM APPLICANTS;