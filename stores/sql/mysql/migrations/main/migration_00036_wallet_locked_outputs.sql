CREATE TABLE `wallet_locked_outputs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `output_id` varbinary(32) NOT NULL,
  `unlock_timestamp` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_wallet_locked_outputs_output_id` (`output_id`),
  KEY `idx_wallet_locked_outputs_unlock_timestamp` (`unlock_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
