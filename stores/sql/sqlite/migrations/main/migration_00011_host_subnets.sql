ALTER TABLE `hosts` ADD COLUMN `subnets` text NOT NULL DEFAULT '';
UPDATE `hosts` SET last_scan = 0;