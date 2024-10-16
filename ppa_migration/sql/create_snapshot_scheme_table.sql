-- Check if the 'Snapshot' table exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Snapshot' AND xtype='U')
BEGIN
    CREATE TABLE dbo.Snapshot (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer
        SnapshotID UNIQUEIDENTIFIER NOT NULL,  -- Unique identifier for Snapshot
        SnapshotName NVARCHAR(200) NULL,  -- Name of the Snapshot
        SnapshotType NVARCHAR(50) NULL,  -- Type of the Snapshot
        CreatedOn DATETIME NULL,  -- Date and time of record creation
        CreatedBy UNIQUEIDENTIFIER NULL,  -- GUID of the user who created the record
        ModifiedOn DATETIME NULL,  -- Date and time of record modification
        ModifiedBy UNIQUEIDENTIFIER NULL,  -- GUID of the user who modified the record
        Status NVARCHAR(50) NULL,  -- Status of the Snapshot record
        
        CONSTRAINT [PK_Snapshot] PRIMARY KEY CLUSTERED 
        (
            [SnapshotID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;
