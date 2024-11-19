ALTER TABLE contracts ADD COLUMN usability tinyint unsigned;
CREATE INDEX `idx_contracts_usability` ON contracts (`usability`);
UPDATE contracts SET usability = CASE WHEN archival_reason IS NULL THEN 2 ELSE 1 END;
ALTER TABLE contracts MODIFY COLUMN usability tinyint unsigned NOT NULL;