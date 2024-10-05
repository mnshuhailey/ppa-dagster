MERGE INTO dbo.study_transformed_v7 AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
AS source (
    StudyID, SnapshotID, StudyName, Education, EducationType, AcademicStartDate, AcademicEndDate, 
    AsnafID, Qualification, SchoolID, SchoolName, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy, 
    Statecode, Status
)
ON target.StudyID = source.StudyID
WHEN MATCHED THEN
    UPDATE SET
        target.SnapshotID = source.SnapshotID,
        target.StudyName = source.StudyName,
        target.Education = source.Education,
        target.EducationType = source.EducationType,
        target.AcademicStartDate = source.AcademicStartDate,
        target.AcademicEndDate = source.AcademicEndDate,
        target.AsnafID = source.AsnafID,
        target.Qualification = source.Qualification,
        target.SchoolID = source.SchoolID,
        target.SchoolName = source.SchoolName,
        target.CreatedOn = source.CreatedOn,
        target.CreatedBy = source.CreatedBy,
        target.ModifiedOn = source.ModifiedOn,
        target.ModifiedBy = source.ModifiedBy,
        target.Statecode = source.Statecode,
        target.Status = source.Status
WHEN NOT MATCHED THEN
    INSERT (
        StudyID, SnapshotID, StudyName, Education, EducationType, AcademicStartDate, AcademicEndDate, 
        AsnafID, Qualification, SchoolID, SchoolName, CreatedOn, CreatedBy, ModifiedOn, ModifiedBy, 
        Statecode, Status
    )
    VALUES (
        source.StudyID, source.SnapshotID, source.StudyName, source.Education, source.EducationType, 
        source.AcademicStartDate, source.AcademicEndDate, source.AsnafID, source.Qualification, source.SchoolID, 
        source.SchoolName, source.CreatedOn, source.CreatedBy, source.ModifiedOn, source.ModifiedBy, 
        source.Statecode, source.Status
    );
