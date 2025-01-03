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

-- MySQL uses row-level locking with INNODB, so we use a loop to insert in
-- batches to avoid running out of space in the locking table.
SET @start_id = 0;
SET @batch_size = 100000;
SET @done = FALSE;

WHILE NOT @done DO
  INSERT INTO host_sectors (updated_at, db_sector_id, db_host_id)
  SELECT NOW(), cs.db_sector_id, c.host_id
  FROM contract_sectors cs
  INNER JOIN contracts c ON cs.db_contract_id = c.id AND c.host_id IS NOT NULL;
  WHERE cs.id > @start_id
  ORDER BY cs.id
  LIMIT @batch_size;

  -- If we inserted fewer rows than batch_size, we’re done.
  IF ROW_COUNT() < @batch_size THEN
    SET @done = TRUE;
  END IF;

  -- Update the start_id for the next batch
  SET @start_id = @start_id + @batch_size;
END WHILE;