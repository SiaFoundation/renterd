-- prune manually before creating trigger
DELETE slabs
FROM slabs
LEFT JOIN slices ON slices.db_slab_id = slabs.id
WHERE slices.db_object_id IS NULL
AND slices.db_multipart_part_id IS NULL
AND slabs.db_buffered_slab_id IS NULL;

-- remove ON DELETE CASCADE from slices
ALTER TABLE slices DROP FOREIGN KEY fk_objects_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_objects_slabs FOREIGN KEY (db_object_id) REFERENCES test.objects(id);

-- dbSlice cleanup trigger
CREATE TRIGGER delete_from_slices_after_objects_delete
BEFORE DELETE
ON objects FOR EACH ROW
DELETE FROM slices
WHERE slices.db_object_id = OLD.id;

-- dbSlab cleanup triggers
CREATE TRIGGER delete_from_slabs_after_slice_delete
AFTER DELETE
ON slices FOR EACH ROW
DELETE FROM slabs
WHERE slabs.id = OLD.db_slab_id
AND slabs.db_buffered_slab_id IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM slices
    WHERE slices.db_slab_id = OLD.db_slab_id
);