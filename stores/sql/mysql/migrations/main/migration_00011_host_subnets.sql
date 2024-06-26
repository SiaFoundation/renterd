ALTER TABLE `hosts` ADD COLUMN `subnets` text;
UPDATE `hosts` SET last_scan = 0;