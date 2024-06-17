ALTER TABLE Applicants
DROP CONSTRAINT fk_AddressID;
ALTER TABLE Applicants
DROP CONSTRAINT fk_UniID;
ALTER TABLE Applicants
DROP CONSTRAINT fk_Talent_CoordinatorID;
ALTER TABLE Applicants DROP COLUMN IF EXISTS Address_ID;
ALTER TABLE Applicants DROP COLUMN IF EXISTS Uni_ID;
ALTER TABLE Applicants DROP COLUMN IF EXISTS Talent_Coordinator_ID;
DROP TABLE IF EXISTS Address;
DROP TABLE IF EXISTS Talent_Coordinators;
DROP TABLE IF EXISTS Uni;


-- ########### Address Table #############
-- SELECT * FROM Address;

CREATE TABLE Address (
    Address_ID INT IDENTITY(1,1) PRIMARY KEY,
    Address_Name VARCHAR(255) NOT NULL,
    City_Name VARCHAR(255) NOT NULL,
    Postcode_Name VARCHAR(100) NOT NULL
);

INSERT INTO Address (Address_Name, City_Name, Postcode_Name) 
SELECT DISTINCT address, city, postcode FROM Talent_CSV 
WHERE address IS NOT NULL AND city IS NOT NULL AND postcode IS NOT NULL;

ALTER TABLE Applicants ADD Address_ID INT;
ALTER TABLE Applicants
ADD CONSTRAINT fk_AddressID
FOREIGN KEY (Address_ID) REFERENCES Address(Address_ID);

UPDATE Applicants
SET Address_ID = (SELECT Address_ID FROM Address WHERE Applicants.city = Address.City_Name AND Applicants.address = Address.Address_Name);

-- SELECT Address_ID, COUNT(Address_ID) FROM Applicants
-- GROUP BY Address_ID
-- ORDER BY COUNT(Address_ID) DESC;

-- ############ CREATE UNI TABLE ##############
-- SELECT * FROM Applicants;
-- SELECT * FROM Uni;
DROP TABLE IF EXISTS Uni;

CREATE TABLE Uni (
    Uni_ID INT IDENTITY(1,1) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

ALTER TABLE Applicants ADD Uni_ID INT;

INSERT INTO Uni (Name) 
SELECT DISTINCT uni FROM Talent_CSV 
WHERE uni IS NOT NULL;

ALTER TABLE Applicants
ADD CONSTRAINT fk_UniID
FOREIGN KEY (Uni_ID) REFERENCES Uni(Uni_ID);

UPDATE Applicants
SET Uni_ID = (SELECT Uni_ID FROM Uni WHERE Applicants.uni = Uni.Name);

-- ##### CREATE TALENT_COORDINATORS TABLE ############
-- Fifi Etton and Fifi Eton could be the same person? With following query below, you can see how many times they appear.
-- SELECT tc.name, COUNT(a.name) FROM Talent_Coordinators tc INNER JOIN Applicants a ON tc.Name = a.invited_by GROUP BY tc.name ORDER BY COUNT(a.name) ASC;

CREATE TABLE Talent_Coordinators (
    Talent_Coordinator_ID INT IDENTITY(1,1) PRIMARY KEY,
    Name VARCHAR(255) NOT NULL);

ALTER TABLE Applicants ADD Talent_Coordinator_ID INT;

INSERT INTO Talent_Coordinators (Name) 
SELECT DISTINCT invited_by
FROM Talent_CSV
WHERE invited_by IS NOT NULL;

ALTER TABLE Applicants
ADD CONSTRAINT fk_Talent_CoordinatorID
FOREIGN KEY (Talent_Coordinator_ID) REFERENCES Talent_Coordinators(Talent_Coordinator_ID);

UPDATE Applicants
SET Talent_Coordinator_ID = (SELECT Talent_Coordinator_ID FROM Talent_Coordinators WHERE Applicants.invited_by = Talent_Coordinators.Name);

SELECT * FROM Applicants;
SELECT * FROM Uni;
SELECT * FROM Address;
SELECT * FROM Talent_Coordinators;