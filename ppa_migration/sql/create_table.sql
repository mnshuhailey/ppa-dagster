IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='asnaf_transformed' AND xtype='U')
CREATE TABLE dbo.asnaf_transformed (
    AsnafID VARCHAR(500) PRIMARY KEY,
    AsnafName VARCHAR(500),
    Emel VARCHAR(500),
    Age INT
);
