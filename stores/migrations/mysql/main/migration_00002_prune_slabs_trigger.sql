-- prune manually before creating trigger
DELETE slabs
FROM slabs
LEFT JOIN slices ON slices.db_slab_id = slabs.id
WHERE slices.db_object_id IS NULL
AND slices.db_multipart_part_id IS NULL
AND slabs.db_buffered_slab_id IS NULL;

-- remove ON DELETE CASCADE from slices
ALTER TABLE slices DROP FOREIGN KEY fk_objects_slabs;
ALTER TABLE slices DROP FOREIGN KEY fk_multipart_parts_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_objects_slabs FOREIGN KEY (db_object_id) REFERENCES objects(id);
ALTER TABLE slices ADD CONSTRAINT fk_multipart_parts_slabs FOREIGN KEY (db_multipart_part_id) REFERENCES multipart_parts(id),

-- remove ON DELETE CASCADE from multipart_uploads
ALTER TABLE multipart_uploads DROP FOREIGN KEY fk_multipart_uploads_parts;
ALTER TABLE multipart_uploads ADD CONSTRAINT fk_multipart_uploads_parts FOREIGN KEY (db_multipart_upload_id) REFERENCES multipart_uploads(id)

-- dbObject trigger to delete from slices
CREATE TRIGGER delete_from_slices_after_objects_delete
BEFORE DELETE
ON objects FOR EACH ROW
DELETE FROM slices
WHERE slices.db_object_id = OLD.id;

-- dbMultipartUpload trigger to delete from dbMultipartPart
CREATE TRIGGER delete_from_multipart_parts_after_multipart_upload_delete
BEFORE DELETE
ON multipart_uploads FOR EACH ROW
DELETE FROM multipart_parts
WHERE multipart_parts.db_multipart_upload_id = OLD.id;

-- dbMultipartPart trigger to delete from slices
CREATE TRIGGER delete_from_slices_after_multipart_parts_delete
BEFORE DELETE
ON multipart_parts FOR EACH ROW
DELETE FROM slices
WHERE slices.db_multipart_part_id = OLD.id;

-- dbSlices trigger to prune slabs
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