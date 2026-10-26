-- uploads
CREATE TABLE `uploads` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) NOT NULL,
  `upload_id` varbinary(8) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_uploads_upload_id` (`upload_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- uploading sectors
CREATE TABLE `uploading_sectors` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `db_upload_id` bigint unsigned NOT NULL,
  `root` varbinary(32) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_uploading_sectors_db_upload_id` (`db_upload_id`),
  CONSTRAINT `fk_uploading_sectors_uploads` FOREIGN KEY (`db_upload_id`) REFERENCES `uploads` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
