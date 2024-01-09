-- dbObjectUserMetadata
CREATE TABLE `object_user_metadata` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_object_id` bigint unsigned NOT NULL,
  `key` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `value` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_object_user_metadata_key` (`db_object_id`,`key`),
  CONSTRAINT `fk_object_user_metadata` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbMultipartUploadUserMetadata
CREATE TABLE `multipart_upload_user_metadatas` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_multipart_upload_id` bigint unsigned NOT NULL,
  `key` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `value` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_multipart_upload_user_metadata_key` (`db_multipart_upload_id`,`key`),
  CONSTRAINT `fk_multipart_upload_user_metadatas` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
