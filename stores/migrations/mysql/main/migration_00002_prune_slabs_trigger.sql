-- prune manually before creating trigger
DELETE slabs
FROM slabs
LEFT JOIN slices ON slices.db_slab_id = slabs.id
WHERE slices.db_object_id IS NULL
AND slices.db_multipart_part_id IS NULL
AND slabs.db_buffered_slab_id IS NULL;

-- dbSlab cleanup triggers
CREATE TRIGGER delete_from_slabs_after_slice_delete
AFTER DELETE
ON slices FOR EACH ROW
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