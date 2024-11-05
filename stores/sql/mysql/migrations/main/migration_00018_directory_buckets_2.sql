ALTER TABLE objects
ADD CONSTRAINT fk_objects_db_directory_id
FOREIGN KEY (db_directory_id) REFERENCES directories(id);
