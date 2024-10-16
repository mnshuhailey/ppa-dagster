-- Check if the 'Study' table exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Study' AND xtype='U')
BEGIN
    CREATE TABLE dbo.Study (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer
        StudyID UNIQUEIDENTIFIER NOT NULL,  -- Unique identifier for study
        SnapshotID UNIQUEIDENTIFIER NULL,  -- Snapshot identifier
        StudyName NVARCHAR(200) NULL,  -- Name of the study
        Education NVARCHAR(50) NULL,  -- Education level
        EducationType NVARCHAR(50) NULL,  -- Type of education
        AcademicStartDate DATETIME NULL,  -- Academic start date
        AcademicEndDate DATETIME NULL,  -- Academic end date
        AsnafID UNIQUEIDENTIFIER NULL,  -- Foreign key for Asnaf
        Qualification NVARCHAR(50) NULL,  -- Qualification obtained
        SchoolID UNIQUEIDENTIFIER NULL,  -- Foreign key for School
        SchoolName NVARCHAR(100) NULL,  -- Name of the school
        CreatedOn DATETIME NULL,  -- Record creation date
        CreatedBy UNIQUEIDENTIFIER NULL,  -- User who created the record
        ModifiedOn DATETIME NULL,  -- Record modification date
        ModifiedBy UNIQUEIDENTIFIER NULL,  -- User who modified the record
        Statecode INT NULL,  -- State code
        Status NVARCHAR(50) NULL,  -- Status of the study record

        CONSTRAINT [PK_Study] PRIMARY KEY CLUSTERED 
        (
            [StudyID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

-- Add foreign key constraint to 'asnaf' table if it doesn't already exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Study_asnaf]'))
BEGIN
    ALTER TABLE [dbo].[Study] WITH CHECK ADD CONSTRAINT [FK_Study_asnaf] 
    FOREIGN KEY([AsnafID]) REFERENCES [dbo].[asnaf] ([AsnafID]);

    ALTER TABLE [dbo].[Study] CHECK CONSTRAINT [FK_Study_asnaf];
END;

-- Add foreign key constraint to 'School' table if it doesn't already exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Study_School]'))
BEGIN
    ALTER TABLE [dbo].[Study] WITH CHECK ADD CONSTRAINT [FK_Study_School] 
    FOREIGN KEY([SchoolID]) REFERENCES [dbo].[School] ([SchoolingID]);

    ALTER TABLE [dbo].[Study] CHECK CONSTRAINT [FK_Study_School];
END;
