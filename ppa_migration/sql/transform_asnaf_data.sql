SELECT
    UPPER(gen_random_uuid()::text) AS "SnapshotID",
    "vwlzs_asnafId"::uuid AS "AsnafID",
    NULL::text AS "AsnafName",
    "CreatedBy"::uuid AS "Createdby",
    TO_CHAR("CreatedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "DateCreated",
    "vwlzs_Status"::int AS "asnafStatus",
    "vwlzs_name"::text AS "Name",
    "vwlzs_IdentificationType"::int AS "IdentificationType",
    TO_CHAR("vwlzs_DateofBirth", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "DateofBirth",
    "vwlzs_Age"::int AS "Age",
    "vwlzs_MaritalStatus"::int AS "MaritalStatus",
    "vwlzs_WorkRemark"::text AS "Working",
    "vwlzs_BankName"::text AS "Bank",
    "vwlzs_OtherName"::text AS "OtherName",
    "vwlzs_nric"::text AS "IdentificationNumIC",
    "vwlzs_PlaceofBirth"::text AS "BirthLocation",
    CASE WHEN "vwlzs_Gender" IN ('true', 'false')
         THEN CASE WHEN "vwlzs_Gender"::boolean IS TRUE THEN 1 ELSE 0 END
         ELSE NULL END AS "Gender",
    TO_CHAR("vwlzs_DateEmbraceIslamMuallafOnly", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "IfMuallafDateInIslam",
    "vwlzs_Salariesallowancesorincomeearned"::money AS "Salary",
    "vwlzs_remarkothers"::text AS "Remarks",
    "vwlzs_ModeofPayment"::int AS "ModeofPayment",
    "vwlzs_BankAccountNo"::text AS "BankAccountNum",
    "vwlzs_Residentialstatus"::int AS "ResidentialStatus",
    "vwlzs_PeriodStayInSelangorYear"::int AS "PeriodStayInSelangorYear",
    "vwlzs_Street1"::text AS "Street1",
    "vwlzs_Street2"::text AS "Street2",
    "vwlzs_Street3"::text AS "Street3",
    "vwlzs_City"::text AS "City",
    "vwlzs_Postcode"::text AS "Postcode",
    NULL::text AS "Branch",
    "vwlzs_Branch"::text AS "BranchID",
    "vwlzs_State"::int AS "State",
    "vwlzs_Country"::int AS "Country",
    "vwlzs_TelephoneNoHome"::text AS "TelephoneNoHome",
    "vwlzs_TelephoneNoOffice"::text AS "TelephoneNoOffice",
    "vwlzs_MobileNo"::text AS "MobilePhoneNum",
    "vwlzs_Email"::text AS "Emel",
    "vwlzs_Schooling"::int AS "Schooling",
    "vwlzs_Education"::int AS "Education",
    "vwlzs_HighestEducation"::int AS "HighestEducation",
    TO_CHAR("vwlzs_DateAsnafRegistered", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "DateofApplication",
    "vwlzs_AsnafPortalId"::int AS "RegistrationPortalId",
    "vwlzs_Status"::int AS "RegistrationStatus",
    NULL::int AS "RegistrationType",
    "vwlzs_ExitAsnafRemarks"::text AS "ExitAsnafRemark",
    "vwlzs_ExitAsnafReason"::text AS "ExitAsnafReason",
    TO_CHAR("vwlzs_ExitDate", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "ExitDate",
    TO_CHAR("vwlzs_DeceasedDate", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "DeceasedDate",
    NULL::text AS "recipientType",
    NULL::text AS "recipient",
    NULL::text AS "District",
    "vwlzs_District"::text AS "DistrictID",
    NULL::text AS "Mosque",
    "vwlzs_Mosque"::uuid AS "MosqueID",
    NULL::text AS "JenisPendaftaran",
    CASE
        WHEN "vwlzs_Status" = '100000000' THEN 'lulus'
        WHEN "vwlzs_Status" = '1' THEN 'Deceased'
        WHEN "vwlzs_Status" = '2' THEN 'Move Out From Selangor'
        WHEN "vwlzs_Status" = '3' THEN 'End of Study'
        WHEN "vwlzs_Status" = '4' THEN 'Unreachable'
        WHEN "vwlzs_Status" = '5' THEN 'Exit Asnaf'
        WHEN "vwlzs_Status" = '6' THEN 'Jailed'
        ELSE NULL
    END AS "Status",
    "statecode"::int AS "Statecode",
    "vwlzs_Status"::text AS "StatusAsnaf",
    NULL::text AS "PostJson",
    NULL::text AS "ReturnJson",
    NULL::text AS "CRMAsnafID",
    NULL::text AS "PINo",
    NULL::text AS "RegType",
    NULL::text AS "CRMReviewID",
    NULL::text AS "CRMRegistrationID",
    "vwlzs_Relationship"::text AS "Relationship",
    "vwlzs_LivingwithFamily"::text AS "MasihTanggungan",
    NULL::text AS "recipientTypeAsnaf",
    NULL::text AS "recipientTypeOrganization",
    NULL::text AS "Ename",
    NULL::text AS "Eposition",
    NULL::text AS "Eoffice",
    NULL::text AS "Erelation",
    NULL::text AS "reviewRemarks",
    NULL::text AS "resetBy",
    "vwlzs_Healthlevel"::int AS "Healthlevel",
    "vwlzs_PhysicalCondition"::int AS "PhysicalCondition",
    "vwlzs_RemarkHealthLevel"::text AS "healthCronic",
    "vwlzs_PhysicalCondition"::text AS "OKU_Physical",
    "vwlzs_TypeofDisability"::text AS "OKU_Type",
    "vwlzs_ReasonofDisability"::text AS "OKU_Cause",
    "vwlzs_TheLevelofDisability"::text AS "OKU_Cause_Level",
    "vwlzs_Condition"::text AS "Uzur_Condition",
    "vwlzs_CareCosts"::text AS "carecosts",
    "vwlzs_MonthlyTotal"::money AS "monthlytotal",
    "vwlzs_Sector"::text AS "sector",
    "vwlzs_WorkRemark"::text AS "remarkwork",
    "vwlzs_vehicle"::text AS "vehicle",
    NULL::text AS "vehicleothers",
    "vwlzs_EmployeeName"::text AS "namamajikan",
    CASE WHEN "vwlzs_FishermenPoultryAgricultural" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_FishermenPoultryAgricultural"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "fishermenpoultryagricultural",
    CASE WHEN "vwlzs_SewingHandicraftsCookingNursery" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_SewingHandicraftsCookingNursery"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "sewinghandicraftscookingnursery",
    CASE WHEN "vwlzs_Service" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_Service"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "service",
    CASE WHEN "vwlzs_OthersSkillsRemarks" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_OthersSkillsRemarks"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "othersskill",
    NULL::text AS "othersskillremarks",
    CASE WHEN "vwlzs_Carpentry" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_Carpentry"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "carpentry",
    CASE WHEN "vwlzs_Business" IN ('true', 'false')
        THEN CASE WHEN "vwlzs_Business"::boolean IS TRUE THEN 1 ELSE 0 END
        ELSE NULL END AS "business",
    NULL::text AS "portalID",
    NULL::text AS "regstatus",
    NULL::money AS "kadarsewa",
    NULL::text AS "amilID",
    "vwlzs_CategoryAsnaf"::text AS "AsnafCategory",
    "ModifiedBy"::uuid AS "ModifiedBy",
    TO_CHAR("ModifiedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "ModifiedOn",
    NULL::int AS "hadkifayahRate",
    NULL::text AS "hadkifayah",
    "vwlzs_SAPCode"::text AS "SAPCode",
    "vwlzs_sapremarks"::text AS "SAPRemarks",
    CASE WHEN "vwlzs_SAPIndicator" IN ('true', 'false')
         THEN CASE WHEN "vwlzs_SAPIndicator"::boolean IS TRUE THEN 1 ELSE 0 END
         ELSE NULL END AS "SAPIndicator",
    CASE WHEN "vwlzs_Blacklisted" IN ('true', 'false')
         THEN CASE WHEN "vwlzs_Blacklisted"::boolean IS TRUE THEN 1 ELSE 0 END
         ELSE NULL END AS "Blacklisted",
    TO_CHAR("vwlzs_TarikhPermulaanMengikutiKFAMKBAM", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "TarikhMulaKFAMKBAM"
FROM dbo.vwlzs_asnaf;
