ALTER TABLE contracts ADD COLUMN usability INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_contracts_usability ON contracts (usability);