SELECT
     "vwlzs_studyId"::uuid AS "StudyID",
     NULL::uuid AS "SnapshotID",
     "vwlzs_name"::text AS "StudyName",
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
     "statuscode"::text AS "Status"
FROM dbo.vwlzs_study;
