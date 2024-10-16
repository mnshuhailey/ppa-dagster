-- Check if the 'School' table exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='School' AND xtype='U')
BEGIN
    CREATE TABLE dbo.School (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer
        SchoolingID UNIQUEIDENTIFIER NOT NULL,  -- Unique identifier for schooling
        SchoolingName NVARCHAR(255) NULL,  -- Name of the schooling
        SnapshotID UNIQUEIDENTIFIER NULL,  -- Snapshot identifier
        AsnafID UNIQUEIDENTIFIER NULL,  -- Foreign key for Asnaf
        Schooling NVARCHAR(255) NULL,  -- General schooling information
        Schooling_TadikaID UNIQUEIDENTIFIER NULL,  -- Unique identifier for Tadika schooling
        Schooling_Kebangsaan NVARCHAR(200) NULL,  -- Name of Kebangsaan schooling
        Schooling_KebangsaanID UNIQUEIDENTIFIER NULL,  -- Unique identifier for Kebangsaan schooling
        Schooling_Agama NVARCHAR(200) NULL,  -- Name of Agama schooling
        Schooling_AgamaID UNIQUEIDENTIFIER NULL,  -- Unique identifier for Agama schooling
        Schooling_MenengahID UNIQUEIDENTIFIER NULL,  -- Unique identifier for Menengah schooling
        IPT_ID UNIQUEIDENTIFIER NULL,  -- Unique identifier for IPT
        Schooling_Tahap NVARCHAR(50) NULL,  -- Schooling level
        Schooling_Datestart DATETIME NULL,  -- Start date for schooling
        Schooling_Dateend DATETIME NULL,  -- End date for schooling
        Education INT NULL,  -- Education level
        hadkifayah NVARCHAR(100) NULL,  -- Had Kifayah status
        hadkifayahRate INT NULL,  -- Had Kifayah rate
        masihTanggungan NVARCHAR(50) NULL,  -- Status of dependency
        Statecode INT NULL,  -- State code
        CreatedOn DATETIME NULL,  -- Date and time of record creation
        CreatedBy UNIQUEIDENTIFIER NULL,  -- GUID of the user who created the record
        ModifiedOn DATETIME NULL,  -- Date and time of record modification
        ModifiedBy UNIQUEIDENTIFIER NULL,  -- GUID of the user who modified the record,
        
        CONSTRAINT [PK_School] PRIMARY KEY CLUSTERED 
        (
            [SchoolingID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;
