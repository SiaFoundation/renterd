-- dbDirectory
CREATE TABLE IF NOT EXISTS `directories` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_parent_id` bigint unsigned,
  `name` varchar(766) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_directories_parent_id` (`db_parent_id`),
  UNIQUE KEY `idx_directories_name` (`name`),
  CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`db_parent_id`) REFERENCES `directories` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbObject: add column and constraint
ALTER TABLE `objects`
ADD COLUMN `db_directory_id` bigint unsigned,
ADD CONSTRAINT `fk_objects_db_directory_id` FOREIGN KEY (`db_directory_id`) REFERENCES `directories` (`id`);