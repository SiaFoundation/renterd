-- drop triggers
DROP TRIGGER IF EXISTS before_delete_on_objects_delete_slices;
DROP TRIGGER IF EXISTS before_delete_on_multipart_uploads_delete_multipart_parts;
DROP TRIGGER IF EXISTS before_delete_on_multipart_parts_delete_slices;
DROP TRIGGER IF EXISTS after_delete_on_slices_delete_slabs;

-- add ON DELETE CASCADE to slices
ALTER TABLE slices DROP FOREIGN KEY fk_objects_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_objects_slabs FOREIGN KEY (db_object_id) REFERENCES objects (id) ON DELETE CASCADE;

ALTER TABLE slices DROP FOREIGN KEY fk_multipart_parts_slabs;
ALTER TABLE slices ADD CONSTRAINT fk_multipart_parts_slabs FOREIGN KEY (db_multipart_part_id) REFERENCES multipart_parts (id) ON DELETE CASCADE;

-- add ON DELETE CASCADE to multipart_parts
ALTER TABLE multipart_parts DROP FOREIGN KEY fk_multipart_uploads_parts;
ALTER TABLE multipart_parts ADD CONSTRAINT fk_multipart_uploads_parts FOREIGN KEY (db_multipart_upload_id) REFERENCES multipart_uploads (id) ON DELETE CASCADE;