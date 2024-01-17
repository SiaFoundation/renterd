-- prune manually before creating trigger
DELETE FROM slabs
WHERE id IN (
  SELECT slabs.id
  FROM slabs
  LEFT JOIN slices ON slices.db_slab_id = slabs.id
  WHERE slices.db_object_id IS NULL
  AND slices.db_multipart_part_id IS NULL
  AND slabs.db_buffered_slab_id IS NULL
);

-- remove ON DELETE CASCADE from slices
PRAGMA foreign_keys=off;
CREATE TABLE `slices_temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer,`object_index` integer,`db_multipart_part_id` integer,`db_slab_id` integer,`offset` integer,`length` integer,CONSTRAINT `fk_objects_slabs` FOREIGN KEY (`db_object_id`) REFERENCES `objects`(`id`),CONSTRAINT `fk_multipart_parts_slabs` FOREIGN KEY (`db_multipart_part_id`) REFERENCES `multipart_parts`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_slabs_slices` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs`(`id`));
INSERT INTO slices_temp SELECT * FROM slices;
DROP TABLE slices;
ALTER TABLE slices_temp RENAME TO slices;

CREATE INDEX `idx_slices_object_index` ON `slices`(`object_index`);
CREATE INDEX `idx_slices_db_object_id` ON `slices`(`db_object_id`);
CREATE INDEX `idx_slices_db_slab_id` ON `slices`(`db_slab_id`);
CREATE INDEX `idx_slices_db_multipart_part_id` ON `slices`(`db_multipart_part_id`);
PRAGMA foreign_keys=on;

-- dbSlice cleanup trigger
CREATE TRIGGER delete_from_slices_after_objects_delete
BEFORE DELETE ON objects
BEGIN
    DELETE FROM slices
    WHERE slices.db_object_id = OLD.id;
END;

-- dbSlab cleanup triggers
CREATE TRIGGER delete_from_slabs_after_slice_delete
AFTER DELETE ON slices
BEGIN
    DELETE FROM slabs
    WHERE slabs.id = OLD.db_slab_id
    AND slabs.db_buffered_slab_id IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM slices
        WHERE slices.db_slab_id = OLD.db_slab_id
    );
END;