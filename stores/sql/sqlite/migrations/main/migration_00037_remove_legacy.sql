DROP INDEX IF EXISTS `idx_hosts_net_address`;
ALTER TABLE `hosts` DROP COLUMN `net_address`;
ALTER TABLE `hosts` DROP COLUMN `price_table`;
ALTER TABLE `hosts` DROP COLUMN `price_table_expiry`;

ALTER TABLE `contracts` DROP COLUMN `v2`;
