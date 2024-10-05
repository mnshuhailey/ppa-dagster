IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='study_transformed_v7' AND xtype='U')
CREATE TABLE dbo.study_transformed_v7 (
    idno INT IDENTITY(1,1) NOT NULL,
    StudyID UNIQUEIDENTIFIER NOT NULL,
    SnapshotID UNIQUEIDENTIFIER NULL,
    StudyName NVARCHAR(200) NULL,
    Education NVARCHAR(50) NULL,
    EducationType NVARCHAR(50) NULL,
    AcademicStartDate DATETIME NULL,
    AcademicEndDate DATETIME NULL,
    AsnafID UNIQUEIDENTIFIER NULL,
    Qualification NVARCHAR(50) NULL,
    SchoolID UNIQUEIDENTIFIER NULL,
    SchoolName NVARCHAR(100) NULL,
    CreatedOn DATETIME NULL,
    CreatedBy UNIQUEIDENTIFIER NULL,
    ModifiedOn DATETIME NULL,
    ModifiedBy UNIQUEIDENTIFIER NULL,
    Statecode INT NULL,
    Status NVARCHAR(50) NULL,
    PRIMARY KEY (idno)
);
