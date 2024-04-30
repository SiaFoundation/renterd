-- dbDirectory
CREATE TABLE `directories` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `parent_id` bigint unsigned,
  `name` varchar(766) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_directories_parent_id` (`parent_id`),
  UNIQUE KEY `idx_directories_name` (`name`),
  CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`parent_id`) REFERENCES `directories` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

ALTER TABLE `objects`
ADD COLUMN `db_directory_id` bigint unsigned NOT NULL;
ADD CONSTRAINT `fk_objects_db_directory_id` FOREIGN KEY (`db_directory_id`) REFERENCES `directories` (`id`);
CREATE INDEX `idx_objects_db_directory_id` ON `objects` (`db_directory_id`);