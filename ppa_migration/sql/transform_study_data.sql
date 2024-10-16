-- SELECT
--      (_airbyte_data->>'vwlzs_studyId')::uuid AS StudyID,
--      UPPER(gen_random_uuid()::text) AS SnapshotID,
--      NULL::text AS StudyName,
--      (_airbyte_data->>'vwlzs_Education')::text AS Education,
--      NULL::text AS EducationType,
--      TO_CHAR((_airbyte_data->>'vwlzs_AcademicStartDate')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS AcademicStartDate,
--      TO_CHAR((_airbyte_data->>'vwlzs_AcademicEndDate')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS AcademicEndDate,
--      (_airbyte_data->>'vwlzs_Asnaf')::uuid AS AsnafID,
--      (_airbyte_data->>'vwlzs_StudyQualification')::text AS Qualification,
--      (_airbyte_data->>'vwlzs_School')::uuid AS SchoolID,
--      (_airbyte_data->>'vwlzs_SchoolName')::text AS SchoolName,
--      TO_CHAR((_airbyte_data->>'CreatedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS CreatedOn,
--      (_airbyte_data->>'CreatedBy')::uuid AS CreatedBy,
--      TO_CHAR((_airbyte_data->>'ModifiedOn')::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS ModifiedOn,
--      (_airbyte_data->>'ModifiedBy')::uuid AS ModifiedBy,
--      (_airbyte_data->>'statecode')::text AS Statecode,
--      (_airbyte_data->>'vwlzs_AcademicStatus')::text AS Status
-- FROM airbyte_internal.dbo_raw__stream_vwlzs_study
-- ORDER BY (_airbyte_data->>'CreatedOn')::timestamp DESC
-- LIMIT 100000;

SELECT
     "vwlzs_studyId"::uuid AS "StudyID",
     NULL::uuid AS "SnapshotID",
     NULL::text AS "StudyName",
     "vwlzs_Education"::text AS "Education",
     NULL::text AS "EducationType",
     TO_CHAR("vwlzs_AcademicStartDate"::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "AcademicStartDate",
     TO_CHAR("vwlzs_AcademicEndDate"::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "AcademicEndDate",
     "vwlzs_Asnaf"::uuid AS "AsnafID",
     "vwlzs_StudyQualification"::text AS "Qualification",
     "vwlzs_School"::uuid AS "SchoolID",
     "vwlzs_SchoolName"::text AS "SchoolName",
     TO_CHAR("CreatedOn"::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "CreatedOn",
     "CreatedBy"::uuid AS "CreatedBy",
     TO_CHAR("ModifiedOn"::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "ModifiedOn",
     "ModifiedBy"::uuid AS "ModifiedBy",
     "statecode"::text AS "Statecode",
     "vwlzs_AcademicStatus"::text AS "Status"
FROM dbo.vwlzs_study;
