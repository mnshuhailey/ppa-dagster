-- Fetch data and transform here
SELECT
    (_airbyte_data->>'vwlzs_asnafId')::text as AsnafID,
    (_airbyte_data->>'vwlzs_AsnafRegistrationIdName')::text as AsnafName,
    (_airbyte_data->>'vwlzs_Email')::text as Emel,
    (_airbyte_data->>'vwlzs_Age')::int as Age
FROM airbyte_internal.dbo_raw__stream_vwlzs_asnaf
LIMIT 1000;
