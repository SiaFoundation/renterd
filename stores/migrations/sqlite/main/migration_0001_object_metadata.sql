-- dbObjectUserMetadata
CREATE TABLE `object_user_metadata` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer NOT NULL,`key` text NOT NULL,`value` text, CONSTRAINT `fk_object_metadata` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_object_metadata_key` ON `object_user_metadata`(`db_object_id`,`key`);

-- dbMultipartUploadUserMetadata
CREATE TABLE `multipart_upload_user_metadatas` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_multipart_upload_id` integer NOT NULL,`key` text NOT NULL,`value` text,CONSTRAINT `fk_multipart_upload_user_metadatas` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_multipart_metadata_key` ON `multipart_upload_user_metadatas`(`db_multipart_upload_id`,`key`);