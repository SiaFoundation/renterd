ALTER TABLE objects DROP FOREIGN KEY fk_objects_db_directory_id;
ALTER TABLE objects DROP COLUMN db_directory_id;
DROP TABLE directories;