ALTER TABLE objects DROP FOREIGN KEY fk_objects_db_directory_id;
DROP TABLE IF EXISTS directories;
CREATE TABLE `directories` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_bucket_id` bigint unsigned NOT NULL,
  `db_parent_id` bigint unsigned,
  `name` varchar(766) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_directories_parent_id` (`db_parent_id`),
  KEY `idx_directories_name` (`name`),
  UNIQUE KEY `idx_directories_bucket_name` (`db_bucket_id`,`name`),
  CONSTRAINT `fk_directories_db_directories` FOREIGN KEY (`db_parent_id`) REFERENCES `directories` (`id`),
  CONSTRAINT `fk_directories_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;