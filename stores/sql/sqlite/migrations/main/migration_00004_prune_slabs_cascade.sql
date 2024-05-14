-- drop triggers
DROP TRIGGER IF EXISTS before_delete_on_objects_delete_slices;
DROP TRIGGER IF EXISTS before_delete_on_multipart_uploads_delete_multipart_parts;
DROP TRIGGER IF EXISTS before_delete_on_multipart_parts_delete_slices;
DROP TRIGGER IF EXISTS after_delete_on_slices_delete_slabs;

PRAGMA foreign_keys=off;
-- update constraints on slices
DROP TABLE IF EXISTS slices_temp;
CREATE TABLE `slices_temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer,`object_index` integer,`db_multipart_part_id` integer,`db_slab_id` integer,`offset` integer,`length` integer,CONSTRAINT `fk_objects_slabs` FOREIGN KEY (`db_object_id`) REFERENCES `objects`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_multipart_parts_slabs` FOREIGN KEY (`db_multipart_part_id`) REFERENCES `multipart_parts`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_slabs_slices` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs`(`id`));
INSERT INTO slices_temp SELECT `id`, `created_at`, `db_object_id`, `object_index`, `db_multipart_part_id`, `db_slab_id`, `offset`, `length` FROM slices;
DROP TABLE slices;
ALTER TABLE slices_temp RENAME TO slices;

CREATE INDEX `idx_slices_object_index` ON `slices`(`object_index`);
CREATE INDEX `idx_slices_db_object_id` ON `slices`(`db_object_id`);
CREATE INDEX `idx_slices_db_slab_id` ON `slices`(`db_slab_id`);
CREATE INDEX `idx_slices_db_multipart_part_id` ON `slices`(`db_multipart_part_id`);

-- update constraints multipart_parts
DROP TABLE IF EXISTS multipart_parts_temp;
CREATE TABLE `multipart_parts_temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`etag` text,`part_number` integer,`size` integer,`db_multipart_upload_id` integer NOT NULL,CONSTRAINT `fk_multipart_uploads_parts` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads`(`id`) ON DELETE CASCADE);
INSERT INTO multipart_parts_temp SELECT * FROM multipart_parts;
DROP TABLE multipart_parts;
ALTER TABLE multipart_parts_temp RENAME TO multipart_parts;

CREATE INDEX `idx_multipart_parts_db_multipart_upload_id` ON `multipart_parts`(`db_multipart_upload_id`);
CREATE INDEX `idx_multipart_parts_part_number` ON `multipart_parts`(`part_number`);
CREATE INDEX `idx_multipart_parts_etag` ON `multipart_parts`(`etag`);
PRAGMA foreign_keys=on;
