-- Average total score of all spartans
SELECT a.Applicant_Name, AVG(sj.score) AS 'Average Score'
FROM Applicants a
INNER JOIN Score_Junction sj
ON a.Applicant_ID = sj.Applicant_ID
GROUP BY a.Applicant_Name
ORDER BY AVG(sj.score) DESC


-- Creating view with output as all streams with 10 weeks, 16 Streams with 10 weeks
create view ten_week_streams 
AS
SELECT DISTINCT Stream_ID FROM
(SELECT s.Stream_ID, scj.week, SCJ.score FROM Streams s
INNER JOIN Streams_Junction sj
ON s.Stream_ID = sj.Stream_ID
INNER JOIN Applicants a
ON sj.Applicant_ID = a.Applicant_ID
INNER JOIN Score_Junction scj
ON a.Applicant_ID = scj.Applicant_ID) WEEKS
WHERE WEEKS.week = 10 and score IS NOT NULL

SELECT* FROM ten_week_streams

-- Creating view with the total average score of spartans on 10 week streams
CREATE VIEW average_ten_week_spartans
AS
SELECT a.Applicant_Name, AVG(scj.score) AS "Total Average Score"
FROM Applicants a 
INNER JOIN Streams_Junction stj 
ON a.Applicant_ID = stj.Applicant_ID 
INNER JOIN Score_Junction scj 
ON a.Applicant_ID = scj.Applicant_ID
WHERE Stream_ID IN (select * from ten_week_streams)
GROUP BY Applicant_Name

-- TOP 10, 10 week spartans
SELECT TOP 10 * FROM average_ten_week_spartans
ORDER BY [Total Average Score] DESC

-- Average score of each behaviour for top 10, 10 week spartans
WITH Top_Spartas AS (
    SELECT a.Applicant_Name, sj.score, b.behaviour
    FROM Applicants a
    INNER JOIN Score_Junction sj
    ON a.Applicant_ID = sj.Applicant_ID
    INNER JOIN Behaviour b 
    ON sj.Behaviour_ID = b.Behaviour_ID
)SELECT *
FROM Top_Spartas PIVOT(AVG(score) FOR behaviour IN([Studious],[Independent],[Imaginative],[Determined],[Analytic],[Professional])) TS
WHERE Applicant_Name IN ('Violet Luscombe', 'Gabbey Caesman', 'Yalonda Beacom', 'Auberon Werny', 'Otes Kemster', 'Reggie Lawlor','Jillian Wenn','Anatole Burston','Elva Boldero','Uriel Eidelman')
;

-- Creating view with the total average score of spartans on 8 week streams
CREATE VIEW average_eight_week_spartans
AS
SELECT a.Applicant_Name, AVG(scj.score) AS "Total Average Score"
FROM Applicants a 
INNER JOIN Streams_Junction stj 
ON a.Applicant_ID = stj.Applicant_ID 
INNER JOIN Score_Junction scj 
ON a.Applicant_ID = scj.Applicant_ID
WHERE Stream_ID NOT IN (select * from ten_week_streams)
GROUP BY Applicant_Name


-- TOP 10, 8 week spartans
SELECT TOP 10 * FROM average_eight_week_spartans
ORDER BY [Total Average Score] DESC

-- Average score of each behaviour for top 10, 8 week spartans
WITH Top_Spartas AS (
    SELECT a.Applicant_Name, sj.score, b.behaviour
    FROM Applicants a
    INNER JOIN Score_Junction sj
    ON a.Applicant_ID = sj.Applicant_ID
    INNER JOIN Behaviour b 
    ON sj.Behaviour_ID = b.Behaviour_ID
)SELECT *
FROM Top_Spartas PIVOT(AVG(score) FOR behaviour IN([Studious],[Independent],[Imaginative],[Determined],[Analytic],[Professional])) TS
WHERE Applicant_Name IN ('Katleen Trunks', 'Anica Vallis', 'Cami Burberow', 'Cristal Jeans', 'Rickard Blakes', 'Benedikt Cohani', 'Kingston Comsty','Matteo Yeowell','Lazare Nellies', 'Florina Sudlow')
;

-- Degree and psychometric/presentation scores of top 10, 10 and 8 week spartans
SELECT Applicant_Name,Degree, Psychometric_Score, Presentation_Score
FROM APPLICANTS 
WHERE Applicant_Name IN ('Violet Luscombe', 'Gabbey Caesman', 'Yalonda Beacom', 'Auberon Werny', 
                        'Otes Kemster', 'Reggie Lawlor','Jillian Wenn','Anatole Burston','Elva Boldero',
                        'Uriel Eidelman','Katleen Trunks', 'Anica Vallis', 'Cami Burberow', 'Cristal Jeans', 
                        'Rickard Blakes', 'Benedikt Cohani', 'Kingston Comsty','Matteo Yeowell','Lazare Nellies', 'Florina Sudlow')
ORDER BY Psychometric_Score DESC, Presentation_Score DESC

-- 10 Lost in 1st week, 18 in 2nd, 13 in 3rd. No Spartans left after week 3. 
SELECT 
SUM(CASE WHEN Week = 1 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left by Week 0",
(SUM(CASE WHEN Week = 2 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 1 AND score IS NULL THEN 1 ELSE 0 END) /6   AS "Left at Week 1",
(SUM(CASE WHEN Week = 3 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 2 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 2",
(SUM(CASE WHEN Week = 4 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 3 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 3",
(SUM(CASE WHEN Week = 5 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 4 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 4",
(SUM(CASE WHEN Week = 6 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 5 AND score IS NULL THEN 1 ELSE 0 END) /6   AS "Left at Week 5",
(SUM(CASE WHEN Week = 7 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 6 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 6",
(SUM(CASE WHEN Week = 8 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 7 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 7",
(SUM(CASE WHEN Week = 9 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 8 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Week 8 Streams End",
(SUM(CASE WHEN Week = 10 AND score IS NULL THEN 1 ELSE 0 END) /6) - SUM(CASE WHEN Week = 9 AND score IS NULL THEN 1 ELSE 0 END) /6  AS "Left at Week 9"
FROM Score_Junction

-- Checking if anyone dropped out on Week 8 on 10 week streams (week 7 is there to check if they had left prior to week 8), no one did 
SELECT a.Applicant_Name, sj.score, sj.week, stj.Stream_ID
FROM Applicants a 
INNER JOIN Score_Junction sj
on a.Applicant_ID = sj.Applicant_ID
INNER JOIN Streams_Junction stj
ON a.Applicant_ID = stj.Applicant_ID
WHERE Stream_ID IN (SELECT * FROM ten_week_streams)
AND score IS NULL AND week BETWEEN 7 and 10


-- Average Number of Spartans per stream is 11, 17 Streams with 12 or more spartans 
SELECT AVG([Number of Spartans]) AS "Average Number of Spartans per Stream"
FROM (
    SELECT COUNT(a.Applicant_ID) as "Number of Spartans" FROM Applicants a
    INNER JOIN Streams_Junction sj
    ON a.Applicant_ID = sj.Applicant_ID
    GROUP BY Stream_ID
   -- ORDER BY [Number of Spartans]
    ) as count

--Most common strengths in order
SELECT s.strength_name, COUNT(*) AS "Number of Applicants with Strength"
FROM applicant_strengths_junction asj
INNER JOIN strengths s
ON asj.strength_id = s.strength_id
GROUP BY s.strength_name
ORDER BY [Number of Applicants with Strength] desc

--Most common weaknesses in order
SELECT w.weakness_name, COUNT(*) AS "Number of Applicants with Weakness"
FROM applicant_weakness_junction awj
INNER JOIN weaknesses w
ON awj.weakness_id = w.weakness_id
GROUP BY w.weakness_name
ORDER BY [Number of Applicants with Weakness] desc

-- Most common skills in order
SELECT ts.tech_name, COUNT(*) AS "Number of Applicants with Skill"
FROM Applicant_Tech_jct atj
INNER JOIN Tech_Skill ts
ON atj.Tech_ID = ts.Tech_ID
GROUP BY ts.Tech_Name
ORDER BY [Number of Applicants with Skill] desc

--- FOR USE IN EXCEL
CREATE VIEW weekly_view2
AS
    SELECT a.Applicant_Name, sj.score, b.behaviour, sj.week, stj.Stream_ID
    FROM Applicants a
    INNER JOIN Score_Junction sj
    ON a.Applicant_ID = sj.Applicant_ID
    INNER JOIN Behaviour b 
    ON sj.Behaviour_ID = b.Behaviour_ID
    INNER JOIN Streams_Junction stj 
    ON a.Applicant_ID = stj.Applicant_ID

SELECT * FROM weekly_view2

