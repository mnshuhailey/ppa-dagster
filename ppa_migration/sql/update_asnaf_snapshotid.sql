UPDATE asnaf
SET asnaf.SnapshotID = familyrelationship.SnapshotID
FROM asnaf
INNER JOIN familyrelationship 
    ON asnaf.AsnafID = familyrelationship.ParticularAsnafID;

