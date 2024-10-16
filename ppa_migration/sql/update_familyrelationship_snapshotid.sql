UPDATE familyrelationship
SET familyrelationship.SnapshotID = household.SnapshotID
FROM familyrelationship
INNER JOIN household 
    ON familyrelationship.HouseholdID = household.HouseholdID;
