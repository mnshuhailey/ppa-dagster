-- Check if the 'Familyrelationship' table exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Familyrelationship' AND xtype='U')
BEGIN
    CREATE TABLE dbo.Familyrelationship (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer, primary key
        FamilyrelationshipID UNIQUEIDENTIFIER NOT NULL,  -- Uniqueidentifier (GUID)
        SnapshotID UNIQUEIDENTIFIER NOT NULL,  -- Snapshot ID (GUID)
        FamilyrelationshipName NVARCHAR(200) NULL,  -- Unicode string with max length of 200 characters
        Status NVARCHAR(50) NULL,  -- Unicode string with max length of 50 characters
        Statecode INT NULL,  -- Integer for state code
        Relationship NVARCHAR(50) NULL,  -- Unicode string for relationship
        KKAsnafID UNIQUEIDENTIFIER NULL,  -- GUID for KK Asnaf
        ParticularAsnafID UNIQUEIDENTIFIER NULL,  -- GUID for Particular Asnaf
        HouseholdID UNIQUEIDENTIFIER NULL,  -- GUID for Household
        CreatedOn DATETIME NULL,  -- Date and time of creation
        CreatedBy UNIQUEIDENTIFIER NULL,  -- GUID for the creator
        ModifiedOn DATETIME NULL,  -- Date and time of modification
        ModifiedBy UNIQUEIDENTIFIER NULL,  -- GUID for the modifier,
        
        CONSTRAINT [PK_Familyrelationship] PRIMARY KEY CLUSTERED 
        (
            [FamilyrelationshipID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

-- Optionally add foreign key constraint for ParticularAsnafID, only if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Familyrelationship_asnaf]') AND parent_object_id = OBJECT_ID(N'[dbo].[Familyrelationship]'))
BEGIN
    ALTER TABLE [dbo].[Familyrelationship] WITH CHECK ADD CONSTRAINT [FK_Familyrelationship_asnaf] 
    FOREIGN KEY([ParticularAsnafID]) REFERENCES [dbo].[asnaf] ([AsnafID]);

    ALTER TABLE [dbo].[Familyrelationship] CHECK CONSTRAINT [FK_Familyrelationship_asnaf];
END;

-- Optionally add foreign key constraint for HouseholdID, only if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Familyrelationship_Household]') AND parent_object_id = OBJECT_ID(N'[dbo].[Familyrelationship]'))
BEGIN
    ALTER TABLE [dbo].[Familyrelationship] WITH CHECK ADD CONSTRAINT [FK_Familyrelationship_Household] 
    FOREIGN KEY([HouseholdID]) REFERENCES [dbo].[Household] ([HouseholdID]);

    ALTER TABLE [dbo].[Familyrelationship] CHECK CONSTRAINT [FK_Familyrelationship_Household];
END;
