ALTER TABLE host_sectors ADD COLUMN `deleted_at` datetime;
CREATE INDEX `idx_host_sectors_deleted_at` ON `host_sectors` (`deleted_at`);
