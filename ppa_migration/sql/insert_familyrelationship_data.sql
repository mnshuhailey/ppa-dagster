MERGE INTO dbo.familyrelationship_transformed_v7 AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
AS source (
    FamilyrelationshipID, SnapshotID, FamilyrelationshipName, Status, Statecode, Relationship, 
    KKAsnafID, ParticularAsnafID, HouseholdID, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
)
ON target.FamilyrelationshipID = source.FamilyrelationshipID
WHEN MATCHED THEN
    UPDATE SET
        target.SnapshotID = source.SnapshotID,
        target.FamilyrelationshipName = source.FamilyrelationshipName,
        target.Status = source.Status,
        target.Statecode = source.Statecode,
        target.Relationship = source.Relationship,
        target.KKAsnafID = source.KKAsnafID,
        target.ParticularAsnafID = source.ParticularAsnafID,
        target.HouseholdID = source.HouseholdID,
        target.CreatedOn = source.CreatedOn,
        target.CreatedBy = source.CreatedBy,
        target.ModifiedOn = source.ModifiedOn,
        target.ModifiedBy = source.ModifiedBy
WHEN NOT MATCHED THEN
    INSERT (
        FamilyrelationshipID, SnapshotID, FamilyrelationshipName, Status, Statecode, Relationship, 
        KKAsnafID, ParticularAsnafID, HouseholdID, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
    )
    VALUES (
        source.FamilyrelationshipID, source.SnapshotID, source.FamilyrelationshipName, source.Status, source.Statecode, source.Relationship, 
        source.KKAsnafID, source.ParticularAsnafID, source.HouseholdID, source.CreatedOn, source.CreatedBy, source.ModifiedOn, source.ModifiedBy
    );
