-- Fetch data from PostgreSQL (for documentation, not executed directly)
SELECT
    (_airbyte_data->>'vwlzs_asnafId')::text as AsnafID,
    (_airbyte_data->>'vwlzs_AsnafRegistrationIdName')::text as AsnafName,
    (_airbyte_data->>'vwlzs_Email')::text as Emel,
    (_airbyte_data->>'vwlzs_Age')::int as Age
FROM airbyte_internal.dbo_raw__stream_vwlzs_asnaf
LIMIT 10;

-- Merge data into SQL Server (for documentation, not executed directly)
MERGE INTO dbo.asnaf_transformed AS target
USING (VALUES (?, ?, ?, ?)) AS source (AsnafID, AsnafName, Emel, Age)
ON target.AsnafID = source.AsnafID
WHEN MATCHED THEN
    UPDATE SET
        target.AsnafName = source.AsnafName,
        target.Emel = source.Emel,
        target.Age = source.Age
WHEN NOT MATCHED THEN
    INSERT (AsnafID, AsnafName, Emel, Age)
    VALUES (source.AsnafID, source.AsnafName, source.Emel, source.Age);
