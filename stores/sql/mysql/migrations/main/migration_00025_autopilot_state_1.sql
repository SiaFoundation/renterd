-- remove references to autopilots table
ALTER TABLE host_checks DROP FOREIGN KEY fk_host_checks_autopilot;
ALTER TABLE host_checks DROP COLUMN db_autopilot_id;
ALTER TABLE host_checks DROP INDEX idx_host_checks_id;
ALTER TABLE host_checks ADD UNIQUE INDEX idx_host_checks_id (db_host_id);

-- create autopilot_state table & insert blank state object
CREATE TABLE `autopilot_state` (
  `id` bigint unsigned NOT NULL DEFAULT 1,
  `created_at` datetime(3) DEFAULT NULL,
  `current_period` bigint unsigned DEFAULT 0,

  `contracts_amount` bigint unsigned DEFAULT NULL,
  `contracts_period` bigint unsigned DEFAULT NULL,
  `contracts_renew_window` bigint unsigned DEFAULT NULL,
  `contracts_download` bigint unsigned DEFAULT NULL,
  `contracts_upload` bigint unsigned DEFAULT NULL,
  `contracts_storage` bigint unsigned DEFAULT NULL,
  `contracts_prune` boolean NOT NULL DEFAULT false,

  `hosts_allow_redundant_ips` boolean NOT NULL DEFAULT false,
  `hosts_max_downtime_hours` bigint unsigned DEFAULT NULL,
  `hosts_min_protocol_version` varchar(191) DEFAULT NULL,
  `hosts_max_consecutive_scan_failures` bigint unsigned DEFAULT NULL,

  PRIMARY KEY (`id`),
  CHECK (`id` = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `autopilot_state` (`id`, `created_at`, `current_period`) VALUES (1, NOW(), 0);
