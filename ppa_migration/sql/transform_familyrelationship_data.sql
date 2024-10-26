SELECT
     "vwlzs_familyrelationshipId"::uuid AS "FamilyrelationshipID",
     UPPER(gen_random_uuid()::text) AS "SnapshotID",
     "vwlzs_name"::text AS "FamilyrelationshipName",
     "vwlzs_Status"::text AS "Status",
     "statecode"::text AS "Statecode",
     "vwlzs_Relationship"::text AS "Relationship",
     "vwlzs_AsnafId"::uuid AS "KKAsnafID",
     "vwlzs_ParticularAsnafId"::uuid AS "ParticularAsnafID",
     "vwlzs_HouseholdId"::uuid AS "HouseholdID",
     TO_CHAR("CreatedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "CreatedOn",
     "CreatedBy"::uuid AS "CreatedBy",
     TO_CHAR("ModifiedOn", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "ModifiedOn",
     "ModifiedBy"::uuid AS "ModifiedBy"
FROM dbo.vwlzs_familyrelationship;

