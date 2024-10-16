-- Check if the 'Hadkifayah' table exists, and create it only if it doesn't
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Hadkifayah' AND xtype='U')
BEGIN
    CREATE TABLE dbo.Hadkifayah (
        idno INT IDENTITY(1,1) NOT NULL,  -- Auto-incrementing integer, primary key
        hadkifayahID UNIQUEIDENTIFIER NOT NULL,  -- Unique identifier for had kifayah
        SnapshotID UNIQUEIDENTIFIER NULL,  -- Snapshot identifier
        hadkifayahName NVARCHAR(200) NULL,  -- Name for had kifayah record
        Status NVARCHAR(50) NULL,  -- Status
        Statecode INT NULL,  -- State code
        BilAnakBelajarDiIPT INT NULL,  -- Number of children studying in IPT
        AnakBelajarDiIPT MONEY NULL,  -- Cost per child studying in IPT
        JumlahAnakBelajarDiIPT MONEY NULL,  -- Total cost for all children studying in IPT
        BilAnakBersekolahberumur517tahun INT NULL,  -- Number of children aged 5-17 years attending school
        AnakBersekolahberumur517tahun MONEY NULL,  -- Cost per child aged 5-17 attending school
        JumlahAnakBersekolahberumur517tahun MONEY NULL,  -- Total cost for all children aged 5-17 attending school
        BilAnakTidakBersekolahberumur17tahunkebaw INT NULL,  -- Number of children aged below 17 years not attending school
        AnakTidakBersekolahberumur17tahunkebawah MONEY NULL,  -- Cost per child aged below 17 not attending school
        JumlahAnakTidakBersekolahberumur17tahunke MONEY NULL,  -- Total cost for all children aged below 17 not attending school
        BilDewasaberumur18tahunkeatasdantinggal INT NULL,  -- Number of adults aged 18 and above living in the household
        Dewasaberumur18tahunkeatasdantinggalbers MONEY NULL,  -- Cost per adult aged 18 and above living in the household
        JumlahDewasaberumur18tahunkeatasdantingg MONEY NULL,  -- Total cost for all adults aged 18 and above living in the household
        BilPasangan INT NULL,  -- Number of spouses
        Pasangan MONEY NULL,  -- Cost per spouse
        JumlahPasangan MONEY NULL,  -- Total cost for all spouses
        BilPasanganTidakBekerja INT NULL,  -- Number of non-working spouses
        PasanganTidakBekerja MONEY NULL,  -- Cost per non-working spouse
        JumlahPasanganTidakBekerja MONEY NULL,  -- Total cost for all non-working spouses
        FreeFullyPaidHouse MONEY NULL,  -- Value of a fully paid house
        HouseUnderInstallmentorRental MONEY NULL,  -- Value of a house under installment or rental
        House NVARCHAR(50) NULL,  -- Type of house
        Rumah NVARCHAR(50) NULL,  -- House in Malay (Rumah)
        TotalHouseholdGrossIncome MONEY NULL,  -- Total gross income of the household
        TotalHouseholdGrossIncomeTotalLiabilities MONEY NULL,  -- Total household gross income minus liabilities
        TotalLiabilities MONEY NULL,  -- Total liabilities
        CostOfChildCare MONEY NULL,  -- Cost of child care
        DependentChildWithDisability MONEY NULL,  -- Cost for dependent children with disabilities
        MedicalCostsOfChronicIllness MONEY NULL,  -- Medical costs for chronic illness
        HouseholdGrossIncomeDTotalHadKifayahE MONEY NULL,  -- Household gross income minus total had kifayah
        householdgrossincomedtotalhadkifayahe_Base MONEY NULL,  -- Base household gross income minus total had kifayah
        TotalHadKifayahE MONEY NULL,  -- Total had kifayah (eligible financial aid)
        totalhadkifayahe_Base MONEY NULL,  -- Base total had kifayah
        TotalHouseholdGrossIncomeD MONEY NULL,  -- Total household gross income
        totalhouseholdgrossincomed_Base MONEY NULL,  -- Base total household gross income
        HouseholdID UNIQUEIDENTIFIER NULL,  -- Household ID
        CategoryAsnaf NVARCHAR(50) NULL,  -- Asnaf category
        CategoryHadKifayah NVARCHAR(50) NULL,  -- Had kifayah category
        AsnafID UNIQUEIDENTIFIER NULL,  -- Asnaf ID
        AsnafReviewID UNIQUEIDENTIFIER NULL,  -- Asnaf review ID
        CreatedOn DATETIME NULL,  -- Record creation date
        CreatedBy UNIQUEIDENTIFIER NULL,  -- GUID of the record creator
        ModifiedOn DATETIME NULL,  -- Record modification date
        ModifiedBy UNIQUEIDENTIFIER NULL,  -- GUID of the record modifier,
        
        CONSTRAINT [PK_Hadkifayah] PRIMARY KEY CLUSTERED 
        (
            [hadkifayahID] ASC
        ) WITH (
            PAD_INDEX = OFF, 
            STATISTICS_NORECOMPUTE = OFF, 
            IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, 
            ALLOW_PAGE_LOCKS = ON
        ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

-- Optionally add foreign key constraint for AsnafID, only if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Hadkifayah_asnaf]') AND parent_object_id = OBJECT_ID(N'[dbo].[Hadkifayah]'))
BEGIN
    ALTER TABLE [dbo].[Hadkifayah] WITH CHECK ADD CONSTRAINT [FK_Hadkifayah_asnaf] 
    FOREIGN KEY([AsnafID]) REFERENCES [dbo].[asnaf] ([AsnafID]);

    ALTER TABLE [dbo].[Hadkifayah] CHECK CONSTRAINT [FK_Hadkifayah_asnaf];
END;

-- Optionally add foreign key constraint for HouseholdID, only if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_Hadkifayah_Household]') AND parent_object_id = OBJECT_ID(N'[dbo].[Hadkifayah]'))
BEGIN
    ALTER TABLE [dbo].[Hadkifayah] WITH CHECK ADD CONSTRAINT [FK_Hadkifayah_Household] 
    FOREIGN KEY([HouseholdID]) REFERENCES [dbo].[Household] ([HouseholdID]);

    ALTER TABLE [dbo].[Hadkifayah] CHECK CONSTRAINT [FK_Hadkifayah_Household];
END;
