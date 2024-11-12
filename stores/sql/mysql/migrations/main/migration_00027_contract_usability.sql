ALTER TABLE contracts ADD COLUMN usability tinyint unsigned;
UPDATE contracts SET usability = CASE WHEN archival_reason IS NULL 1 ELSE 2 END;
ALTER TABLE contracts MODIFY COLUMN usability tinyint unsigned NOT NULL;
CREATE INDEX `idx_contracts_usability` ON contracts (`usability`);