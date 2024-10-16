-- Check if the table 'Household' exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Household' AND xtype='U')
BEGIN
    CREATE TABLE dbo.Household (
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
        CONSTRAINT [PK_Household] PRIMARY KEY CLUSTERED 
        (
            [HouseholdID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;
