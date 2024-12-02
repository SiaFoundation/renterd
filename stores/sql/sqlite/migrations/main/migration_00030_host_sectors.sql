CREATE TABLE `host_sectors` (`updated_at` datetime, `db_sector_id` integer,`db_host_id` integer,PRIMARY KEY (`db_sector_id`,`db_host_id`),CONSTRAINT `fk_host_sectors_db_sector` FOREIGN KEY (`db_sector_id`) REFERENCES `sectors`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_contract_sectors_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_host_sectors_updated_at` ON `host_sectors`(`updated_at`);
CREATE INDEX `idx_host_sectors_db_host_id` ON `host_sectors`(`db_host_id`);
CREATE INDEX `idx_host_sectors_db_sector_id` ON `host_sectors`(`db_sector_id`);

INSERT OR IGNORE INTO host_sectors (updated_at, db_sector_id, db_host_id)
SELECT DATETIME('now'), cs.db_sector_id, c.host_id
FROM contract_sectors cs
INNER JOIN contracts c ON cs.db_contract_id = c.id AND c.host_id IS NOT NULL