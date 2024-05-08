-- dbDirectory
DROP TABLE IF EXISTS `directories`;
CREATE TABLE `directories` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_parent_id` integer,`name` text, CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`db_parent_id`) REFERENCES `directories`(`id`));
CREATE INDEX `idx_directories_parent_id` ON `directories`(`db_parent_id`);
CREATE UNIQUE INDEX `idx_directories_name` ON `directories`(`name`);

-- dbObject: add column and constraint
DROP TABLE IF EXISTS `objects_temp`;
CREATE TABLE `objects_temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_bucket_id` integer NOT NULL, `db_directory_id` integer NOT NULL, `object_id` text,`key` blob,`health` real NOT NULL DEFAULT 1,`size` integer,`mime_type` text,`etag` text,CONSTRAINT `fk_objects_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets`(`id`),CONSTRAINT `fk_objects_db_directories` FOREIGN KEY (`db_directory_id`) REFERENCES `directories`(`id`));

INSERT INTO `objects_temp` (`id`, `created_at`, `db_bucket_id`, `db_directory_id`, `object_id`, `key`, `health`, `size`, `mime_type`, `etag`)
SELECT `id`, `created_at`, `db_bucket_id`, 0, `object_id`, `key`, `health`, `size`, `mime_type`, `etag`
FROM `objects`;
DROP TABLE `objects`;
ALTER TABLE `objects_temp` RENAME TO `objects`;

CREATE INDEX `idx_objects_db_bucket_id` ON `objects`(`db_bucket_id`);
CREATE INDEX `idx_objects_etag` ON `objects`(`etag`);
CREATE INDEX `idx_objects_health` ON `objects`(`health`);
CREATE INDEX `idx_objects_object_id` ON `objects`(`object_id`);
CREATE INDEX `idx_objects_size` ON `objects`(`size`);
CREATE UNIQUE INDEX `idx_object_bucket` ON `objects`(`db_bucket_id`,`object_id`);
CREATE INDEX `idx_objects_created_at` ON `objects`(`created_at`);
