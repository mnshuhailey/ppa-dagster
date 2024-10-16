-- SELECT
--      (_airbyte_data->>'vwlzs_householdId')::uuid AS HouseholdID,
--      UPPER(gen_random_uuid()::text) AS SnapshotID,
--      (_airbyte_data->>'vwlzs_name')::text AS HouseholdName,
--      (_airbyte_data->>'vwlzs_Status')::text AS Status,
--      (_airbyte_data->>'statecode')::text AS Statecode,
--      (_airbyte_data->>'vwlzs_HeadofFamilyId')::uuid AS HeadofFamilyId,
--      (_airbyte_data->>'vwlzs_Branch')::uuid AS Branch,
--      (_airbyte_data->>'vwlzs_District')::uuid AS District,
--      (_airbyte_data->>'vwlzs_Kariah')::uuid AS Kariah,
--      TO_CHAR((_airbyte_data->>'CreatedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS CreatedOn,
--      (_airbyte_data->>'CreatedBy')::uuid AS CreatedBy,
--      TO_CHAR((_airbyte_data->>'ModifiedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS ModifiedOn,
--      (_airbyte_data->>'ModifiedBy')::uuid AS ModifiedBy
-- FROM airbyte_internal.dbo_raw__stream_vwlzs_household
-- ORDER BY (_airbyte_data->>'CreatedOn')::timestamp DESC
-- LIMIT 100000;

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
