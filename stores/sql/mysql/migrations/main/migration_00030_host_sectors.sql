DROP TABLE IF EXISTS `host_sectors`;

CREATE TABLE `host_sectors` (
  `updated_at` datetime(3) DEFAULT NULL,
  `db_sector_id` bigint unsigned NOT NULL,
  `db_host_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`db_sector_id`, `db_host_id`),
  KEY `idx_host_sectors_updated_at` (`updated_at`),
  KEY `idx_host_sectors_db_sector_id` (`db_sector_id`),
  KEY `idx_host_sectors_db_host_id` (`db_host_id`),
  CONSTRAINT `fk_host_sectors_db_sector` FOREIGN KEY (`db_sector_id`) REFERENCES `sectors` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_host_sectors_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

LOCK TABLES contracts READ, contract_sectors READ, host_sectors WRITE;

INSERT IGNORE INTO host_sectors (updated_at, db_sector_id, db_host_id)
SELECT NOW(), contract_sectors.db_sector_id, contracts.host_id
FROM contract_sectors
INNER JOIN contracts ON contract_sectors.db_contract_id = contracts.id AND contracts.host_id IS NOT NULL;

UNLOCK TABLES;
