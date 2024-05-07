PRAGMA defer_foreign_keys=ON;

-- dbDirectory
DROP TABLE IF EXISTS `directories`;
CREATE TABLE `directories` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_parent_id` integer,`name` text, CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`db_parent_id`) REFERENCES `directories`(`id`));
CREATE INDEX `idx_directories_parent_id` ON `directories`(`db_parent_id`);
CREATE UNIQUE INDEX `idx_directories_parent_id_name` ON `directories`(`db_parent_id`, `name`);

-- dbObject: add column and constraint
ALTER TABLE `objects`
ADD COLUMN `db_directory_id` integer DEFAULT 0 NOT NULL,
ADD CONSTRAINT `fk_objects_db_directory_id` FOREIGN KEY (`db_directory_id`) REFERENCES `directories` (`id`);