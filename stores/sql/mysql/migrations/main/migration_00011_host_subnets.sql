ALTER TABLE `hosts` ADD COLUMN `subnets` text NOT NULL;
UPDATE `hosts` SET last_scan = 0;