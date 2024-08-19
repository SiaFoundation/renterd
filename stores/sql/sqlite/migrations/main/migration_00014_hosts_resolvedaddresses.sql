ALTER TABLE hosts DROP COLUMN subnets;
ALTER TABLE hosts ADD resolved_addresses TEXT NOT NULL DEFAULT '';
