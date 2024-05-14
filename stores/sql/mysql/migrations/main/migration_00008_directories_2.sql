-- Add NOT NULL to column
ALTER TABLE `objects`
MODIFY COLUMN `db_directory_id` bigint unsigned NOT NULL;