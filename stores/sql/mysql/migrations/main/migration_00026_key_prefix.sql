ALTER TABLE `multipart_uploads` MODIFY `key` binary(33) NOT NULL;

UPDATE `multipart_uploads`
SET `key` = CONCAT(x'01', LEFT(`key`, 32));
