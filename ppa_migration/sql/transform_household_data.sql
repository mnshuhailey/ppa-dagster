SELECT
     "vwlzs_householdId"::uuid AS "HouseholdID",
     NULL::uuid AS "SnapshotID",
     "vwlzs_name"::text AS "HouseholdName",
     "vwlzs_Status"::text AS "Status",
     "statecode"::text AS "Statecode",
     "vwlzs_HeadofFamilyId"::uuid AS "HeadofFamilyId",
     "vwlzs_Branch"::uuid AS "Branch",
     "vwlzs_District"::uuid AS "District",
     "vwlzs_Kariah"::uuid AS "Kariah",
     TO_CHAR("CreatedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "CreatedOn",
     "CreatedBy"::uuid AS "CreatedBy",
     TO_CHAR("ModifiedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "ModifiedOn",
     "ModifiedBy"::uuid AS "ModifiedBy"
FROM dbo.vwlzs_household;
