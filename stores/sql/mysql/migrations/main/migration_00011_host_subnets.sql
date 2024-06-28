ALTER TABLE `hosts` ADD COLUMN `subnets` VARCHAR(255) NOT NULL DEFAULT '';
UPDATE `hosts` SET last_scan = 0;