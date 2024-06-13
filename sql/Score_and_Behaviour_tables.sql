DROP TABLE IF EXISTS Score_Junction
DROP TABLE IF EXISTS Behaviour

SELECT name, behaviour, week, score
INTO Score_Junction
FROM Score
WHERE name IS NOT NULL;

-- Add Applicant_ID column to Score_Junction table:
ALTER TABLE Score_Junction ADD Applicant_ID INT;
-- replace the name with its corresponding Applicant_ID:
UPDATE Score_Junction
SET Applicant_ID = (SELECT Applicant_ID FROM Applicants WHERE Applicants.name = Score_JUnction.name);

-- Remove name column from Score_Junction table:
ALTER TABLE Score_Junction DROP COLUMN name;

-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN Applicant_ID INT NOT NULL;

-- Add a foreign key to the Applicant_ID column in Score table:
ALTER TABLE Score_Junction
ADD CONSTRAINT fk_Applicant_ID
FOREIGN KEY (Applicant_ID) REFERENCES Applicants(Applicant_ID);

-- Create Behaviour table
SELECT DISTINCT Behaviour_ID = IDENTITY(INT, 1, 1), Behaviour
INTO Behaviour
FROM Score_Junction
WHERE Behaviour IS NOT NULL;

-- Add PK to Behaviour_ID:
ALTER TABLE Behaviour
ADD CONSTRAINT Behaviour_ID PRIMARY KEY (Behaviour_ID);

-- Add Behaviour_ID column to Score_Junction table:
ALTER TABLE Score_Junction ADD Behaviour_ID INT;
-- replace the Behaviour with its corresponding Behaviour_ID:
UPDATE Score_Junction
SET Behaviour_ID = (SELECT Behaviour_ID FROM Behaviour WHERE Behaviour.Behaviour = Score_Junction.Behaviour);


-- Remove Behaviour column from Score_Junction table:
ALTER TABLE Score_Junction DROP COLUMN Behaviour;

-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN Behaviour_ID INT NOT NULL;

-- Add NOT NULL constraint:
ALTER TABLE Score_Junction
ALTER COLUMN week INT NOT NULL;

-- Add a foreign key to the Behaviour_ID column in Score_Junction table:
ALTER TABLE Score_Junction
ADD CONSTRAINT fk_Behaviour_ID
FOREIGN KEY (Behaviour_ID) REFERENCES Behaviour(Behaviour_ID);

-- change foreign keys to composite key
ALTER TABLE Score_Junction
ADD CONSTRAINT pk_Score_Junction_ID PRIMARY KEY (Applicant_ID, Behaviour_ID, week);
