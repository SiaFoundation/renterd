-- prune manually before creating trigger
DELETE slabs
FROM slabs
LEFT JOIN slices ON slices.db_slab_id = slabs.id
WHERE slices.db_object_id IS NULL
AND slices.db_multipart_part_id IS NULL
AND slabs.db_buffered_slab_id IS NULL;

-- remove ON DELETE CASCADE from slices
ALTER TABLE slices DROP FOREIGN KEY fk_objects_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_objects_slabs FOREIGN KEY (db_object_id) REFERENCES objects(id);

ALTER TABLE slices DROP FOREIGN KEY fk_multipart_parts_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_multipart_parts_slabs FOREIGN KEY (db_multipart_part_id) REFERENCES multipart_parts(id);

-- remove ON DELETE CASCADE from multipart_parts
ALTER TABLE multipart_parts DROP FOREIGN KEY fk_multipart_uploads_parts;
ALTER TABLE multipart_parts ADD CONSTRAINT fk_multipart_uploads_parts FOREIGN KEY (db_multipart_upload_id) REFERENCES multipart_uploads(id);

-- dbObject trigger to delete from slices
CREATE TRIGGER before_delete_on_objects_delete_slices
BEFORE DELETE
ON objects FOR EACH ROW
DELETE FROM slices
WHERE slices.db_object_id = OLD.id;

-- dbMultipartUpload trigger to delete from dbMultipartPart
CREATE TRIGGER before_delete_on_multipart_uploads_delete_multipart_parts
BEFORE DELETE
ON multipart_uploads FOR EACH ROW
DELETE FROM multipart_parts
WHERE multipart_parts.db_multipart_upload_id = OLD.id;

-- dbMultipartPart trigger to delete from slices
CREATE TRIGGER before_delete_on_multipart_parts_delete_slices
BEFORE DELETE
ON multipart_parts FOR EACH ROW
DELETE FROM slices
WHERE slices.db_multipart_part_id = OLD.id;

-- dbSlices trigger to prune slabs
CREATE TRIGGER after_delete_on_slices_delete_slabs
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