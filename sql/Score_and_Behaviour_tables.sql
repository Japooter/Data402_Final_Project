DROP TABLE IF EXISTS Behaviour

-- Do not run 'drop table if exists Score'
-- Drop extra columns from Score table (cleaner version of Academy CSV):
ALTER TABLE Score DROP COLUMN Category, Stream, Date, trainer;

-- Add Applicant_ID column to Score table:
ALTER TABLE Score ADD Applicant_ID INT;
-- replace the name with its corresponding Applicant_ID:
UPDATE Score
SET Applicant_ID = (SELECT Applicant_ID FROM Applicants WHERE Applicants.name = Score.ACname);

-- Remove ACname column from Score table:
ALTER TABLE Score DROP COLUMN Acname;

-- Add a foreign key to the Applicant_ID column in Score table:
ALTER TABLE Score
ADD CONSTRAINT fk_Applicant_ID
FOREIGN KEY (Applicant_ID) REFERENCES Applicants(Applicant_ID);

-- Create Behaviour table
SELECT DISTINCT Behaviour_ID = IDENTITY(INT, 1, 1), Behaviour
INTO Behaviour
FROM Score
WHERE Behaviour IS NOT NULL;

-- Add PK to Behaviour_ID:
ALTER TABLE Behaviour
ADD CONSTRAINT Behaviour_ID PRIMARY KEY (Behaviour_ID);


-- Add Behaviour_ID column to Score table:
ALTER TABLE Score ADD Behaviour_ID INT;
-- replace the Behaviour with its corresponding Behaviour_ID:
UPDATE Score
SET Behaviour_ID = (SELECT Behaviour_ID FROM Behaviour WHERE Behaviour.Behaviour = Score.Behaviour);

-- Remove Behaviour column from Score table:
ALTER TABLE Score DROP COLUMN Behaviour;

-- Add a foreign key to the Behaviour_ID column in Score table:
ALTER TABLE Score
ADD CONSTRAINT fk_Behaviour_ID
FOREIGN KEY (Behaviour_ID) REFERENCES Behaviour(Behaviour_ID);