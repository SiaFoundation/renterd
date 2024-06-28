-- dbContractPruneMetric
CREATE TABLE `contract_prunes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `fcid` varbinary(32) NOT NULL,
  `host` varbinary(32) NOT NULL,
  `host_version` varchar(191) DEFAULT NULL,
  `pruned` bigint NOT NULL,
  `remaining` bigint NOT NULL,
  `duration` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_contract_prunes_timestamp` (`timestamp`),
  KEY `idx_contract_prunes_fc_id` (`fcid`),
  KEY `idx_contract_prunes_host` (`host`),
  KEY `idx_contract_prunes_host_version` (`host_version`),
  KEY `idx_contract_prunes_pruned` (`pruned`),
  KEY `idx_contract_prunes_remaining` (`remaining`),
  KEY `idx_contract_prunes_duration` (`duration`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbContractSetMetric
CREATE TABLE `contract_sets` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `name` varchar(191) NOT NULL,
  `contracts` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_contract_sets_timestamp` (`timestamp`),
  KEY `idx_contract_sets_name` (`name`),
  KEY `idx_contract_sets_contracts` (`contracts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbContractSetChurnMetric
CREATE TABLE `contract_sets_churn` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `name` varchar(191) NOT NULL,
  `fc_id` varbinary(32) NOT NULL,
  `direction` varchar(191) NOT NULL,
  `reason` varchar(191) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_contract_sets_churn_timestamp` (`timestamp`),
  KEY `idx_contract_sets_churn_name` (`name`),
  KEY `idx_contract_sets_churn_fc_id` (`fc_id`),
  KEY `idx_contract_sets_churn_direction` (`direction`),
  KEY `idx_contract_sets_churn_reason` (`reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbContractMetric
CREATE TABLE `contracts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `fcid` varbinary(32) NOT NULL,
  `host` varbinary(32) NOT NULL,
  `remaining_collateral_lo` bigint NOT NULL,
  `remaining_collateral_hi` bigint NOT NULL,
  `remaining_funds_lo` bigint NOT NULL,
  `remaining_funds_hi` bigint NOT NULL,
  `revision_number` bigint NOT NULL,
  `upload_spending_lo` bigint NOT NULL,
  `upload_spending_hi` bigint NOT NULL,
  `download_spending_lo` bigint NOT NULL,
  `download_spending_hi` bigint NOT NULL,
  `fund_account_spending_lo` bigint NOT NULL,
  `fund_account_spending_hi` bigint NOT NULL,
  `delete_spending_lo` bigint NOT NULL,
  `delete_spending_hi` bigint NOT NULL,
  `list_spending_lo` bigint NOT NULL,
  `list_spending_hi` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_contracts_fc_id` (`fcid`),
  KEY `idx_contracts_host` (`host`),
  KEY `idx_remaining_collateral` (`remaining_collateral_lo`,`remaining_collateral_hi`),
  KEY `idx_contracts_revision_number` (`revision_number`),
  KEY `idx_upload_spending` (`upload_spending_lo`,`upload_spending_hi`),
  KEY `idx_download_spending` (`download_spending_lo`,`download_spending_hi`),
  KEY `idx_fund_account_spending` (`fund_account_spending_lo`,`fund_account_spending_hi`),
  KEY `idx_contracts_timestamp` (`timestamp`),
  KEY `idx_remaining_funds` (`remaining_funds_lo`,`remaining_funds_hi`),
  KEY `idx_delete_spending` (`delete_spending_lo`,`delete_spending_hi`),
  KEY `idx_list_spending` (`list_spending_lo`,`list_spending_hi`),
  KEY `idx_contracts_fcid_timestamp` (`fcid`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbPerformanceMetric
CREATE TABLE `performance` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `action` varchar(191) NOT NULL,
  `host` varbinary(32) NOT NULL,
  `origin` varchar(191) NOT NULL,
  `duration` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_performance_host` (`host`),
  KEY `idx_performance_origin` (`origin`),
  KEY `idx_performance_duration` (`duration`),
  KEY `idx_performance_timestamp` (`timestamp`),
  KEY `idx_performance_action` (`action`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbWalletMetric
CREATE TABLE `wallets` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `confirmed_lo` bigint NOT NULL,
  `confirmed_hi` bigint NOT NULL,
  `spendable_lo` bigint NOT NULL,
  `spendable_hi` bigint NOT NULL,
  `unconfirmed_lo` bigint NOT NULL,
  `unconfirmed_hi` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_wallets_timestamp` (`timestamp`),
  KEY `idx_confirmed` (`confirmed_lo`,`confirmed_hi`),
  KEY `idx_spendable` (`spendable_lo`,`spendable_hi`),
  KEY `idx_unconfirmed` (`unconfirmed_lo`,`unconfirmed_hi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;