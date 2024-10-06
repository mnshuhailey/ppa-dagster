SELECT
     (_airbyte_data->>'vwlzs_householdId')::uuid AS HouseholdID,
     UPPER(gen_random_uuid()::text) AS SnapshotID,
     (_airbyte_data->>'vwlzs_name')::text AS HouseholdName,
     (_airbyte_data->>'vwlzs_Status')::text AS Status,
     (_airbyte_data->>'statecode')::text AS Statecode,
     (_airbyte_data->>'vwlzs_HeadofFamilyId')::uuid AS HeadofFamilyId,
     (_airbyte_data->>'vwlzs_Branch')::uuid AS Branch,
     (_airbyte_data->>'vwlzs_District')::uuid AS District,
     (_airbyte_data->>'vwlzs_Kariah')::uuid AS Kariah,
     TO_CHAR((_airbyte_data->>'CreatedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS CreatedOn,
     (_airbyte_data->>'CreatedBy')::uuid AS CreatedBy,
     TO_CHAR((_airbyte_data->>'ModifiedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS ModifiedOn,
     (_airbyte_data->>'ModifiedBy')::uuid AS ModifiedBy
FROM airbyte_internal.dbo_raw__stream_vwlzs_household;
