UPDATE household
SET household.SnapshotID = familyrelationship.SnapshotID
FROM household
INNER JOIN familyrelationship 
    ON household.HouseholdID = familyrelationship.HouseholdID;
