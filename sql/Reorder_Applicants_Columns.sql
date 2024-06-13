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
ADD FOREIGN KEY (Uni_ID) REFERENCES Uni(Uni_ID)

ALTER TABLE Applicants
ADD FOREIGN KEY (Talent_Coordinator_ID) REFERENCES Talent_Coordinator(Talent_Coordinator_ID)