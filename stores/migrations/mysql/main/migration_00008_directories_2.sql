-- Add NOT NULL to column
ALTER TABLE `objects`
MODIFY COLUMN `db_directory_id` bigint unsigned NOT NULL;

-- Create index
CREATE INDEX `idx_objects_db_directory_id` ON `objects` (`db_directory_id`);