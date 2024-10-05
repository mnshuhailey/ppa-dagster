MERGE INTO dbo.household_transformed_v7 AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
AS source (
    HouseholdID, SnapshotID, HouseholdName, Status, Statecode, HeadofFamilyId, Branch, 
    District, Kariah, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
)
ON target.HouseholdID = source.HouseholdID
WHEN MATCHED THEN
    UPDATE SET
        target.SnapshotID = source.SnapshotID,
        target.HouseholdName = source.HouseholdName,
        target.Status = source.Status,
        target.Statecode = source.Statecode,
        target.HeadofFamilyId = source.HeadofFamilyId,
        target.Branch = source.Branch,
        target.District = source.District,
        target.Kariah = source.Kariah,
        target.CreatedOn = source.CreatedOn,
        target.CreatedBy = source.CreatedBy,
        target.ModifiedOn = source.ModifiedOn,
        target.ModifiedBy = source.ModifiedBy
WHEN NOT MATCHED THEN
    INSERT (
        HouseholdID, SnapshotID, HouseholdName, Status, Statecode, HeadofFamilyId, Branch, 
        District, Kariah, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
    )
    VALUES (
        source.HouseholdID, source.SnapshotID, source.HouseholdName, source.Status, source.Statecode, 
        source.HeadofFamilyId, source.Branch, source.District, source.Kariah, source.CreatedOn, 
        source.CreatedBy, source.ModifiedOn, source.ModifiedBy
    );
