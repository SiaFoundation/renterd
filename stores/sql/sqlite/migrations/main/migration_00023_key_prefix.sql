UPDATE `objects`
SET `key` = CONCAT(x'01', `key`);
UPDATE `slabs`
SET `key` = CONCAT(x'01', `key`);
