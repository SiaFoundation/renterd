ALTER TABLE contracts ADD COLUMN v2 BOOLEAN DEFAULT FALSE;
CREATE INDEX `idx_contracts_v2` ON `contracts`(`v2`);
