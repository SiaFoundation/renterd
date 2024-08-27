ALTER TABLE hosts DROP COLUMN subnets;
ALTER TABLE hosts ADD resolved_addresses varchar(255) NOT NULL DEFAULT '';
