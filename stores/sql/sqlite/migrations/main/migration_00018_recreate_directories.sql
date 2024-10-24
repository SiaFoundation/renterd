-- scope directories to buckets, note that this migration continues in code to rewire objects to directories
DROP TABLE IF EXISTS `directories`;
CREATE TABLE `directories` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_bucket_id` integer NOT NULL,`db_parent_id` integer,`name` text,CONSTRAINT `fk_objects_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets`(`id`), CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`db_parent_id`) REFERENCES `directories`(`id`));
CREATE INDEX `idx_directories_parent_id` ON `directories`(`db_parent_id`);
CREATE INDEX `idx_directories_name` ON `directories`(`name`);
CREATE UNIQUE INDEX `idx_directories_bucket_name` ON `directories`(`db_bucket_id`,`name`);