-- drop tables
DROP TABLE IF EXISTS `siacoin_elements`;
DROP TABLE IF EXISTS `transactions`;

-- drop column
ALTER TABLE `consensus_infos` DROP COLUMN `cc_id`;

-- dbWalletEvent
CREATE TABLE `wallet_events` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `event_id` varbinary(32) NOT NULL,
  `height` bigint unsigned DEFAULT NULL,
  `block_id` varbinary(32) NOT NULL,
  `inflow` longtext,
  `outflow` longtext,
  `type` varchar(191) NOT NULL,
  `data` longblob NOT NULL,
  `maturity_height` bigint unsigned DEFAULT NULL,
  `timestamp` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `event_id` (`event_id`),
  KEY `idx_wallet_events_maturity_height` (`maturity_height`),
  KEY `idx_wallet_events_type` (`type`),
  KEY `idx_wallet_events_timestamp` (`timestamp`),
  KEY `idx_wallet_events_block_id_height` (`block_id`, `height`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbWalletOutput
CREATE TABLE `wallet_outputs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `output_id` varbinary(32) NOT NULL,
  `leaf_index` bigint,
  `merkle_proof` longblob NOT NULL,
  `value` longtext,
  `address` varbinary(32) DEFAULT NULL,
  `maturity_height` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `output_id` (`output_id`),
  KEY `idx_wallet_outputs_maturity_height` (`maturity_height`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
