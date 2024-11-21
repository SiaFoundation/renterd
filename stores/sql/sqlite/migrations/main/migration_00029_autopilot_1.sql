-- recreate host_checks
CREATE TABLE `host_checks_temp` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `created_at` datetime, `db_host_id` INTEGER NOT NULL, `usability_blocked` INTEGER NOT NULL DEFAULT 0, `usability_offline` INTEGER NOT NULL DEFAULT 0, `usability_low_score` INTEGER NOT NULL DEFAULT 0, `usability_redundant_ip` INTEGER NOT NULL DEFAULT 0, `usability_gouging` INTEGER NOT NULL DEFAULT 0, `usability_not_accepting_contracts` INTEGER NOT NULL DEFAULT 0, `usability_not_announced` INTEGER NOT NULL DEFAULT 0, `usability_not_completing_scan` INTEGER NOT NULL DEFAULT 0, `score_age` REAL NOT NULL, `score_collateral` REAL NOT NULL, `score_interactions` REAL NOT NULL, `score_storage_remaining` REAL NOT NULL, `score_uptime` REAL NOT NULL, `score_version` REAL NOT NULL, `score_prices` REAL NOT NULL, `gouging_contract_err` TEXT, `gouging_download_err` TEXT, `gouging_gouging_err` TEXT, `gouging_prune_err` TEXT, `gouging_upload_err` TEXT, FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE);
INSERT INTO `host_checks_temp` SELECT `id`, `created_at`, `db_host_id`, `usability_blocked`, `usability_offline`, `usability_low_score`, `usability_redundant_ip`, `usability_gouging`, `usability_not_accepting_contracts`, `usability_not_announced`, `usability_not_completing_scan`, `score_age`, `score_collateral`, `score_interactions`, `score_storage_remaining`, `score_uptime`, `score_version`, `score_prices`, `gouging_contract_err`, `gouging_download_err`, `gouging_gouging_err`, `gouging_prune_err`, `gouging_upload_err` FROM `host_checks`;
DROP TABLE `host_checks`;
ALTER TABLE `host_checks_temp` RENAME TO `host_checks`;

CREATE UNIQUE INDEX `idx_host_checks_id` ON `host_checks` (`db_host_id`);
CREATE INDEX `idx_host_checks_usability_blocked` ON `host_checks` (`usability_blocked`);
CREATE INDEX `idx_host_checks_usability_offline` ON `host_checks` (`usability_offline`);
CREATE INDEX `idx_host_checks_usability_low_score` ON `host_checks` (`usability_low_score`);
CREATE INDEX `idx_host_checks_usability_redundant_ip` ON `host_checks` (`usability_redundant_ip`);
CREATE INDEX `idx_host_checks_usability_gouging` ON `host_checks` (`usability_gouging`);
CREATE INDEX `idx_host_checks_usability_not_accepting_contracts` ON `host_checks` (`usability_not_accepting_contracts`);
CREATE INDEX `idx_host_checks_usability_not_announced` ON `host_checks` (`usability_not_announced`);
CREATE INDEX `idx_host_checks_usability_not_completing_scan` ON `host_checks` (`usability_not_completing_scan`);
CREATE INDEX `idx_host_checks_score_age` ON `host_checks` (`score_age`);
CREATE INDEX `idx_host_checks_score_collateral` ON `host_checks` (`score_collateral`);
CREATE INDEX `idx_host_checks_score_interactions` ON `host_checks` (`score_interactions`);
CREATE INDEX `idx_host_checks_score_storage_remaining` ON `host_checks` (`score_storage_remaining`);
CREATE INDEX `idx_host_checks_score_uptime` ON `host_checks` (`score_uptime`);
CREATE INDEX `idx_host_checks_score_version` ON `host_checks` (`score_version`);
CREATE INDEX `idx_host_checks_score_prices` ON `host_checks` (`score_prices`);

-- create autopilot config table
CREATE TABLE autopilot_config (id INTEGER PRIMARY KEY CHECK (id = 1), created_at datetime, enabled integer NOT NULL DEFAULT 0, contracts_amount integer, contracts_period integer, contracts_renew_window integer, contracts_download integer, contracts_upload integer, contracts_storage integer, contracts_prune integer NOT NULL DEFAULT 0, hosts_allow_redundant_ips integer NOT NULL DEFAULT 0, hosts_max_downtime_hours integer, hosts_min_protocol_version text, hosts_max_consecutive_scan_failures integer);