IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='familyrelationship_transformed_v7' AND xtype='U')
CREATE TABLE dbo.familyrelationship_transformed_v7 (
    idno INT IDENTITY(1,1) NOT NULL,
    FamilyrelationshipID UNIQUEIDENTIFIER NOT NULL,
    SnapshotID UNIQUEIDENTIFIER NOT NULL,
    FamilyrelationshipName NVARCHAR(200) NULL,
    Status NVARCHAR(50) NULL,
    Statecode INT NULL,
    Relationship NVARCHAR(50) NULL,
    KKAsnafID UNIQUEIDENTIFIER NULL,
    ParticularAsnafID UNIQUEIDENTIFIER NULL,
    HouseholdID UNIQUEIDENTIFIER NULL,
    CreatedOn DATETIME NULL,
    CreatedBy UNIQUEIDENTIFIER NULL,
    ModifiedOn DATETIME NULL,
    ModifiedBy UNIQUEIDENTIFIER NULL,
    PRIMARY KEY (idno)
);
