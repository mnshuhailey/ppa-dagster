MERGE INTO dbo.School AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
AS source (
    SchoolingID, SchoolingName, SnapshotID, AsnafID, Schooling, Schooling_TadikaID, 
    Schooling_Kebangsaan, Schooling_KebangsaanID, Schooling_Agama, Schooling_AgamaID, 
    Schooling_MenengahID, IPT_ID, Schooling_Tahap, Schooling_Datestart, Schooling_Dateend, 
    Education, hadkifayah, hadkifayahRate, masihTanggungan, Statecode, 
    CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
)
ON target.SchoolingID = source.SchoolingID
WHEN MATCHED THEN
    UPDATE SET
        target.SchoolingName = source.SchoolingName,
        target.SnapshotID = source.SnapshotID,
        target.AsnafID = source.AsnafID,
        target.Schooling = source.Schooling,
        target.Schooling_TadikaID = source.Schooling_TadikaID,
        target.Schooling_Kebangsaan = source.Schooling_Kebangsaan,
        target.Schooling_KebangsaanID = source.Schooling_KebangsaanID,
        target.Schooling_Agama = source.Schooling_Agama,
        target.Schooling_AgamaID = source.Schooling_AgamaID,
        target.Schooling_MenengahID = source.Schooling_MenengahID,
        target.IPT_ID = source.IPT_ID,
        target.Schooling_Tahap = source.Schooling_Tahap,
        target.Schooling_Datestart = source.Schooling_Datestart,
        target.Schooling_Dateend = source.Schooling_Dateend,
        target.Education = source.Education,
        target.hadkifayah = source.hadkifayah,
        target.hadkifayahRate = source.hadkifayahRate,
        target.masihTanggungan = source.masihTanggungan,
        target.Statecode = source.Statecode,
        target.CreatedOn = source.CreatedOn,
        target.CreatedBy = source.CreatedBy,
        target.ModifiedOn = source.ModifiedOn,
        target.ModifiedBy = source.ModifiedBy
WHEN NOT MATCHED THEN
    INSERT (
        SchoolingID, SchoolingName, SnapshotID, AsnafID, Schooling, Schooling_TadikaID, 
        Schooling_Kebangsaan, Schooling_KebangsaanID, Schooling_Agama, Schooling_AgamaID, 
        Schooling_MenengahID, IPT_ID, Schooling_Tahap, Schooling_Datestart, Schooling_Dateend, 
        Education, hadkifayah, hadkifayahRate, masihTanggungan, Statecode, 
        CreatedOn, CreatedBy, ModifiedOn, ModifiedBy
    )
    VALUES (
        source.SchoolingID, source.SchoolingName, source.SnapshotID, source.AsnafID, source.Schooling, 
        source.Schooling_TadikaID, source.Schooling_Kebangsaan, source.Schooling_KebangsaanID, 
        source.Schooling_Agama, source.Schooling_AgamaID, source.Schooling_MenengahID, source.IPT_ID, 
        source.Schooling_Tahap, source.Schooling_Datestart, source.Schooling_Dateend, source.Education, 
        source.hadkifayah, source.hadkifayahRate, source.masihTanggungan, source.Statecode, 
        source.CreatedOn, source.CreatedBy, source.ModifiedOn, source.ModifiedBy
    );
