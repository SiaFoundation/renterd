ALTER TABLE `objects` MODIFY `key` binary(33) NOT NULL;
ALTER TABLE `slabs` MODIFY `key` binary(33) NOT NULL;

UPDATE `objects`
SET `key` = CONCAT(x'01', LEFT(`key`, 32));
UPDATE `slabs`
SET `key` = CONCAT(x'01', LEFT(`key`, 32));