-- Check if the table 'asnaf' exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='asnaf' AND xtype='U')
BEGIN
    CREATE TABLE dbo.asnaf (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing primary key
        SnapshotID UNIQUEIDENTIFIER NOT NULL,  -- Snapshot identifier
        AsnafID UNIQUEIDENTIFIER NOT NULL,  -- Unique identifier for Asnaf
        AsnafName NVARCHAR(255),  -- Name of the Asnaf
        CreatedBy UNIQUEIDENTIFIER,  -- GUID of the user who created the record
        DateCreated DATETIME DEFAULT (GETDATE()),  -- Date and time the record was created, default to current date
        asnafStatus INT,  -- Status of the Asnaf
        Name NVARCHAR(255),  -- Name field
        IdentificationType INT,  -- Type of identification (e.g., passport, IC)
        DateofBirth DATETIME,  -- Date of birth
        Age INT,  -- Age of the Asnaf
        MaritalStatus INT,  -- Marital status
        Working NVARCHAR(255),  -- Occupation or work status
        Bank NVARCHAR(100),  -- Bank name
        OtherName NVARCHAR(255),  -- Other names if any
        IdentificationNumIC NVARCHAR(MAX),  -- IC or other identification number
        BirthLocation NVARCHAR(255),  -- Place of birth
        Gender INT,  -- Gender code (e.g., Male = 1, Female = 2)
        IfMuallafDateInIslam DATETIME,  -- Date of conversion to Islam (if applicable)
        Salary MONEY,  -- Salary
        Remarks NVARCHAR(MAX),  -- Additional remarks
        ModeofPayment INT,  -- Mode of payment code
        BankAccountNum NVARCHAR(MAX),  -- Bank account number
        ResidentialStatus INT,  -- Residential status code
        PeriodStayInSelangorYear INT,  -- Number of years stayed in Selangor
        Street1 NVARCHAR(255),  -- Address line 1
        Street2 NVARCHAR(255),  -- Address line 2
        Street3 NVARCHAR(255),  -- Address line 3
        City NVARCHAR(255),  -- City
        Postcode NVARCHAR(MAX),  -- Postal code
        Branch NVARCHAR(MAX),  -- Branch
        BranchID NVARCHAR(255),  -- Branch identifier
        State INT,  -- State code
        Country INT,  -- Country code
        TelephoneNoHome NVARCHAR(100),  -- Home phone number
        TelephoneNoOffice NVARCHAR(100),  -- Office phone number
        MobilePhoneNum NVARCHAR(100),  -- Mobile phone number
        Emel NVARCHAR(100),  -- Email
        Schooling INT,  -- Schooling status
        Education INT,  -- Education level
        HighestEducation INT,  -- Highest education level
        DateofApplication DATETIME,  -- Date of application
        RegistrationPortalId INT,  -- Registration portal ID
        RegistrationStatus INT,  -- Registration status
        RegistrationType INT,  -- Registration type
        ExitAsnafRemark NVARCHAR(MAX),  -- Remark for exiting Asnaf status
        ExitAsnafReason NVARCHAR(MAX),  -- Reason for exiting Asnaf status
        ExitDate DATETIME,  -- Date of exit
        DeceasedDate DATETIME,  -- Date of death (if applicable)
        recipientType NVARCHAR(100),  -- Type of recipient
        recipient NVARCHAR(100),  -- Recipient
        District NVARCHAR(100),  -- District name
        DistrictID NVARCHAR(255),  -- District identifier
        Mosque NVARCHAR(255),  -- Mosque name
        MosqueID NVARCHAR(255),  -- Mosque identifier
        JenisPendaftaran NVARCHAR(50),  -- Registration type
        Status NVARCHAR(50),  -- Status
        Statecode INT,  -- State code
        StatusAsnaf NVARCHAR(50),  -- Status as Asnaf
        PostJson TEXT,  -- JSON data (if any)
        ReturnJson TEXT,  -- Returned JSON data (if any)
        CRMAsnafID NVARCHAR(100),  -- CRM Asnaf identifier
        PINo NVARCHAR(100) DEFAULT (NULL),  -- PI number, default to NULL
        RegType NVARCHAR(100) DEFAULT (NULL),  -- Registration type, default to NULL
        CRMReviewID NVARCHAR(100) DEFAULT (NULL),  -- CRM Review identifier, default to NULL
        CRMRegistrationID NVARCHAR(100),  -- CRM Registration identifier
        Relationship NVARCHAR(100) DEFAULT (NULL),  -- Relationship status, default to NULL
        MasihTanggungan NVARCHAR(10) DEFAULT (NULL),  -- Whether still dependent, default to NULL
        recipientTypeAsnaf NVARCHAR(100),  -- Type of recipient as Asnaf
        recipientTypeOrganization NVARCHAR(100),  -- Type of recipient as organization
        Ename NVARCHAR(100),  -- Emergency contact name
        Eposition NVARCHAR(100),  -- Emergency contact position
        Eoffice NVARCHAR(100),  -- Emergency contact office
        Erelation NVARCHAR(100),  -- Emergency contact relationship
        reviewRemarks NVARCHAR(MAX),  -- Remarks for review
        resetBy NVARCHAR(100),  -- Reset by whom
        Healthlevel INT,  -- Health level
        PhysicalCondition INT,  -- Physical condition
        healthCronic NVARCHAR(MAX),  -- Chronic health conditions
        OKU_Physical NVARCHAR(100),  -- Physical OKU status
        OKU_Type NVARCHAR(100),  -- OKU type
        OKU_Cause NVARCHAR(100),  -- Cause of OKU status
        OKU_Cause_Level NVARCHAR(100),  -- Severity level of OKU cause
        Uzur_Condition NVARCHAR(100),  -- Uzur condition
        carecosts NVARCHAR(100),  -- Care costs
        monthlytotal NVARCHAR(100),  -- Total monthly costs
        sector NVARCHAR(100),  -- Employment sector
        remarkwork NVARCHAR(MAX),  -- Remarks about work
        vehicle NVARCHAR(100),  -- Vehicle type
        vehicleothers NVARCHAR(100),  -- Other vehicle details
        namamajikan NVARCHAR(MAX),  -- Employer name
        fishermenpoultryagricultural NVARCHAR(100),  -- Industry type (fisherman, poultry, agricultural)
        sewinghandicraftscookingnursery NVARCHAR(100),  -- Industry type (sewing, handicrafts, cooking, nursery)
        service NVARCHAR(MAX),  -- Service details
        othersskill NVARCHAR(MAX),  -- Other skills
        othersskillremarks NVARCHAR(MAX),  -- Remarks about other skills
        carpentry NVARCHAR(100),  -- Carpentry skills
        business NVARCHAR(100),  -- Business details
        portalID NVARCHAR(MAX) DEFAULT (NULL),  -- Portal ID, default to NULL
        regstatus NVARCHAR(50),  -- Registration status
        kadarsewa MONEY,  -- Rent rate
        amilID NVARCHAR(50),  -- Amil ID
        AsnafCategory NVARCHAR(50),  -- Asnaf category
        ModifiedBy UNIQUEIDENTIFIER,  -- Last modified by
        ModifiedOn DATETIME,  -- Last modified on
        hadkifayahRate INT,  -- Had Kifayah rate
        hadkifayah NVARCHAR(100),  -- Had Kifayah status
        SAPCode NVARCHAR(MAX),  -- SAP code
        SAPRemarks NVARCHAR(MAX),  -- SAP remarks
        SAPIndicator INT,  -- SAP indicator
        Blacklisted INT,  -- Blacklist status
        TarikhMulaKFAMKBAM DATETIME,  -- Start date for KFAM/KMBAM
        
        CONSTRAINT [PK_asnaf] PRIMARY KEY CLUSTERED 
        (
            [AsnafID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    );
END;

-- Check if the foreign key exists before creating it
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_asnaf_Snapshot]') AND parent_object_id = OBJECT_ID(N'[dbo].[asnaf]'))
BEGIN
    -- Add foreign key constraint
    ALTER TABLE [dbo].[asnaf] WITH CHECK ADD CONSTRAINT [FK_asnaf_Snapshot] 
    FOREIGN KEY([SnapshotID]) REFERENCES [dbo].[Snapshot] ([SnapshotID]);

    ALTER TABLE [dbo].[asnaf] CHECK CONSTRAINT [FK_asnaf_Snapshot];
END;
