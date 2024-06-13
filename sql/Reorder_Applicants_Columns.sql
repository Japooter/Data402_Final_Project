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
FROM Applicants;
ORDER BY Applicant_ID

EXEC sp_rename 'Applicants', 'Dropped'
EXEC sp_rename 'TempTable', 'Applicants'