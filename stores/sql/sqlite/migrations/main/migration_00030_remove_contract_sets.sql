-- drop default contract set from settings
UPDATE settings SET value=JSON_REMOVE(value, '$.defaultContractSet') WHERE `key`="upload";

-- remove contract set reference from slabs table
CREATE TABLE `slabs_temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_buffered_slab_id` integer DEFAULT NULL,`health` real NOT NULL DEFAULT 1,`health_valid_until` integer NOT NULL DEFAULT 0,`key` blob NOT NULL UNIQUE,`min_shards` integer,`total_shards` integer,CONSTRAINT `fk_buffered_slabs_db_slab` FOREIGN KEY (`db_buffered_slab_id`) REFERENCES `buffered_slabs`(`id`));
INSERT INTO slabs_temp SELECT id, created_at, db_buffered_slab_id, health, health_valid_until, key, min_shards, total_shards FROM slabs;
DROP TABLE slabs;
ALTER TABLE slabs_temp RENAME TO slabs;

CREATE INDEX `idx_slabs_total_shards` ON `slabs`(`total_shards`);
CREATE INDEX `idx_slabs_min_shards` ON `slabs`(`min_shards`);
CREATE INDEX `idx_slabs_health_valid_until` ON `slabs`(`health_valid_until`);
CREATE INDEX `idx_slabs_health` ON `slabs`(`health`);
CREATE INDEX `idx_slabs_db_buffered_slab_id` ON `slabs`(`db_buffered_slab_id`);

-- update usability
UPDATE contracts SET usability = CASE WHEN id IN (SELECT db_contract_id FROM contract_set_contracts) THEN 2 ELSE 1 END;

-- drop contract set tables
DROP TABLE contract_set_contracts;
DROP TABLE contract_sets;
