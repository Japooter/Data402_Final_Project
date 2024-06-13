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
   Applicant_ID int FOREIGN KEY REFERENCES Applicants(Applicant_ID) NOT NULL,
   Tech_ID int FOREIGN KEY REFERENCES Tech_Skill(Tech_ID) NOT NULL,
   Score int NOT NULL
)

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