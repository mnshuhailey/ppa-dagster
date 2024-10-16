-- SELECT
--      (_airbyte_data->>'vwlzs_familyrelationshipId')::uuid AS FamilyrelationshipID,
--      UPPER(gen_random_uuid()::text) AS SnapshotID,
--      (_airbyte_data->>'vwlzs_FamilyIdName')::text AS FamilyrelationshipName,
--      (_airbyte_data->>'vwlzs_Status')::text AS Status,
--      (_airbyte_data->>'statecode')::text AS Statecode,
--      (_airbyte_data->>'vwlzs_Relationship')::text AS Relationship,
--      (_airbyte_data->>'vwlzs_FamilyId')::uuid AS KKAsnafID,
--      (_airbyte_data->>'vwlzs_ParticularAsnafId')::uuid AS ParticularAsnafID,
--      (_airbyte_data->>'vwlzs_HouseholdId')::uuid AS HouseholdID,
--      TO_CHAR((_airbyte_data->>'CreatedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS CreatedOn,
--      (_airbyte_data->>'CreatedBy')::uuid AS CreatedBy,
--      TO_CHAR((_airbyte_data->>'ModifiedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS ModifiedOn,
--      (_airbyte_data->>'ModifiedBy')::uuid AS ModifiedBy
-- FROM airbyte_internal.dbo_raw__stream_vwlzs_familyrelationship
-- ORDER BY (_airbyte_data->>'CreatedOn')::timestamp DESC
-- LIMIT 100000;

SELECT
     "vwlzs_familyrelationshipId"::uuid AS "FamilyrelationshipID",
     UPPER(gen_random_uuid()::text) AS "SnapshotID",
     "vwlzs_FamilyIdName"::text AS "FamilyrelationshipName",
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

