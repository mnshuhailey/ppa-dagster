MERGE INTO dbo.asnaf_transformed_v3 AS target
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
