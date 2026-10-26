-- uploads
CREATE TABLE `uploads` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime NOT NULL,
    `upload_id` blob NOT NULL
);
CREATE UNIQUE INDEX `idx_uploads_upload_id` ON `uploads`(`upload_id`);

-- uploading sectors
CREATE TABLE `uploading_sectors` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `db_upload_id` integer NOT NULL,
    `root` blob NOT NULL,
    CONSTRAINT `fk_uploading_sectors_uploads` FOREIGN KEY (`db_upload_id`) REFERENCES `uploads`(`id`) ON DELETE CASCADE
);
CREATE INDEX `idx_uploading_sectors_db_upload_id` ON `uploading_sectors`(`db_upload_id`);
