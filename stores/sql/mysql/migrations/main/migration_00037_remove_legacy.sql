ALTER TABLE `hosts` DROP INDEX idx_hosts_net_address;
ALTER TABLE `hosts` DROP COLUMN net_address;
ALTER TABLE `hosts` DROP COLUMN price_table;
ALTER TABLE `hosts` DROP COLUMN price_table_expiry;

DELETE FROM `contracts` WHERE `v2` IS FALSE;
ALTER TABLE `contracts` DROP COLUMN `v2`;
