-- dbHostCheck
CREATE TABLE `host_checks` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,

  `db_autopilot_id` bigint unsigned NOT NULL,
  `db_host_id` bigint unsigned NOT NULL,

  `usability_blocked` boolean NOT NULL DEFAULT false,
  `usability_offline` boolean NOT NULL DEFAULT false,
  `usability_low_score` boolean NOT NULL DEFAULT false,
  `usability_redundant_ip` boolean NOT NULL DEFAULT false,
  `usability_gouging` boolean NOT NULL DEFAULT false,
  `usability_not_accepting_contracts` boolean NOT NULL DEFAULT false,
  `usability_not_announced` boolean NOT NULL DEFAULT false,
  `usability_not_completing_scan` boolean NOT NULL DEFAULT false,

  `score_age` double NOT NULL,
  `score_collateral` double NOT NULL,
  `score_interactions` double NOT NULL,
  `score_storage_remaining` double NOT NULL,
  `score_uptime` double NOT NULL,
  `score_version` double NOT NULL,
  `score_prices` double NOT NULL,

  `gouging_contract_err` text,
  `gouging_download_err` text,
  `gouging_gouging_err` text,
  `gouging_prune_err` text,
  `gouging_upload_err` text,

  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_host_checks_id` (`db_autopilot_id`, `db_host_id`),
  INDEX `idx_host_checks_usability_blocked` (`usability_blocked`),
  INDEX `idx_host_checks_usability_offline` (`usability_offline`),
  INDEX `idx_host_checks_usability_low_score` (`usability_low_score`),
  INDEX `idx_host_checks_usability_redundant_ip` (`usability_redundant_ip`),
  INDEX `idx_host_checks_usability_gouging` (`usability_gouging`),
  INDEX `idx_host_checks_usability_not_accepting_contracts` (`usability_not_accepting_contracts`),
  INDEX `idx_host_checks_usability_not_announced` (`usability_not_announced`),
  INDEX `idx_host_checks_usability_not_completing_scan` (`usability_not_completing_scan`),
  INDEX `idx_host_checks_score_age` (`score_age`),
  INDEX `idx_host_checks_score_collateral` (`score_collateral`),
  INDEX `idx_host_checks_score_interactions` (`score_interactions`),
  INDEX `idx_host_checks_score_storage_remaining` (`score_storage_remaining`),
  INDEX `idx_host_checks_score_uptime` (`score_uptime`),
  INDEX `idx_host_checks_score_version` (`score_version`),
  INDEX `idx_host_checks_score_prices` (`score_prices`),

  CONSTRAINT `fk_host_checks_autopilot` FOREIGN KEY (`db_autopilot_id`) REFERENCES `autopilots` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_host_checks_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
