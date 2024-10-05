IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='household_transformed_v7' AND xtype='U')
CREATE TABLE dbo.household_transformed_v7 (
    idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer, primary key
    HouseholdID UNIQUEIDENTIFIER NOT NULL,  -- Uniqueidentifier (GUID)
    SnapshotID UNIQUEIDENTIFIER NULL,
    HouseholdName NVARCHAR(200) NULL,  -- Unicode string with max length of 200 characters
    Status NVARCHAR(50) NULL,  -- Unicode string with max length of 50 characters
    Statecode INT NULL,  -- Integer for state code
    HeadofFamilyId UNIQUEIDENTIFIER NULL,
    Branch UNIQUEIDENTIFIER NULL,
    District UNIQUEIDENTIFIER NULL,
    Kariah UNIQUEIDENTIFIER NULL,
    CreatedOn DATETIME NULL,  -- Date and time
    CreatedBy UNIQUEIDENTIFIER NULL,  -- GUID for the creator
    ModifiedOn DATETIME NULL,  -- Date and time for modification
    ModifiedBy UNIQUEIDENTIFIER NULL,  -- GUID for the modifier
    CONSTRAINT PK_Household PRIMARY KEY ([idno])  -- Primary key constraint on idno
);
