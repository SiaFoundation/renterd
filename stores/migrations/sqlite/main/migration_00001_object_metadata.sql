-- dbObjectUserMetadata
CREATE TABLE `object_user_metadata` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer DEFAULT NULL,`db_multipart_upload_id` integer DEFAULT NULL,`key` text NOT NULL,`value` text, CONSTRAINT `fk_object_user_metadata` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE, CONSTRAINT `fk_multipart_upload_user_metadata` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE SET NULL);
CREATE UNIQUE INDEX `idx_object_user_metadata_key` ON `object_user_metadata`(`db_object_id`,`db_multipart_upload_id`,`key`);
