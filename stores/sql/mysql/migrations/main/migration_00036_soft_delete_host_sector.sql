ALTER TABLE host_sectors ADD COLUMN `deleted_at` datetime(3) DEFAULT NULL;
CREATE INDEX `idx_host_sectors_deleted_at` ON `host_sectors` (`deleted_at`);
