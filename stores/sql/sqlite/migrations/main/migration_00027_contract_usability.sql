ALTER TABLE contracts ADD COLUMN usability INTEGER NOT NULL;
CREATE INDEX idx_contracts_usability ON contracts (usability);
UPDATE contracts SET usability = 1 WHERE 1=1;