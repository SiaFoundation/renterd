-- dbContractMetric
CREATE TABLE `contracts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`fcid` blob NOT NULL,`host` blob NOT NULL,`remaining_collateral_lo` BIGINT NOT NULL,`remaining_collateral_hi` BIGINT NOT NULL,`remaining_funds_lo` BIGINT NOT NULL,`remaining_funds_hi` BIGINT NOT NULL,`revision_number` BIGINT NOT NULL,`upload_spending_lo` BIGINT NOT NULL,`upload_spending_hi` BIGINT NOT NULL,`download_spending_lo` BIGINT NOT NULL,`download_spending_hi` BIGINT NOT NULL,`fund_account_spending_lo` BIGINT NOT NULL,`fund_account_spending_hi` BIGINT NOT NULL,`delete_spending_lo` BIGINT NOT NULL,`delete_spending_hi` BIGINT NOT NULL,`list_spending_lo` BIGINT NOT NULL,`list_spending_hi` BIGINT NOT NULL);
CREATE INDEX `idx_list_spending` ON `contracts`(`list_spending_lo`,`list_spending_hi`);
CREATE INDEX `idx_fund_account_spending` ON `contracts`(`fund_account_spending_lo`,`fund_account_spending_hi`);
CREATE INDEX `idx_contracts_fc_id` ON `contracts`(`fcid`);
CREATE INDEX `idx_remaining_collateral` ON `contracts`(`remaining_collateral_lo`,`remaining_collateral_hi`);
CREATE INDEX `idx_contracts_host` ON `contracts`(`host`);
CREATE INDEX `idx_contracts_timestamp` ON `contracts`(`timestamp`);
CREATE INDEX `idx_delete_spending` ON `contracts`(`delete_spending_lo`,`delete_spending_hi`);
CREATE INDEX `idx_download_spending` ON `contracts`(`download_spending_lo`,`download_spending_hi`);
CREATE INDEX `idx_upload_spending` ON `contracts`(`upload_spending_lo`,`upload_spending_hi`);
CREATE INDEX `idx_contracts_revision_number` ON `contracts`(`revision_number`);
CREATE INDEX `idx_remaining_funds` ON `contracts`(`remaining_funds_lo`,`remaining_funds_hi`);
CREATE INDEX `idx_contracts_fcid_timestamp` ON `contracts`(`fcid`,`timestamp`);

-- dbContractPruneMetric
CREATE TABLE `contract_prunes` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`fcid` blob NOT NULL,`host` blob NOT NULL,`host_version` text,`pruned` BIGINT NOT NULL,`remaining` BIGINT NOT NULL,`duration` integer NOT NULL);
CREATE INDEX `idx_contract_prunes_duration` ON `contract_prunes`(`duration`);
CREATE INDEX `idx_contract_prunes_remaining` ON `contract_prunes`(`remaining`);
CREATE INDEX `idx_contract_prunes_pruned` ON `contract_prunes`(`pruned`);
CREATE INDEX `idx_contract_prunes_host_version` ON `contract_prunes`(`host_version`);
CREATE INDEX `idx_contract_prunes_host` ON `contract_prunes`(`host`);
CREATE INDEX `idx_contract_prunes_fc_id` ON `contract_prunes`(`fcid`);
CREATE INDEX `idx_contract_prunes_timestamp` ON `contract_prunes`(`timestamp`);

-- dbContractSetMetric
CREATE TABLE `contract_sets` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`name` text NOT NULL,`contracts` integer NOT NULL);
CREATE INDEX `idx_contract_sets_timestamp` ON `contract_sets`(`timestamp`);
CREATE INDEX `idx_contract_sets_contracts` ON `contract_sets`(`contracts`);
CREATE INDEX `idx_contract_sets_name` ON `contract_sets`(`name`);

-- dbContractSetChurnMetric
CREATE TABLE `contract_sets_churn` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`name` text NOT NULL,`fc_id` blob NOT NULL,`direction` text NOT NULL,`reason` text NOT NULL);
CREATE INDEX `idx_contract_sets_churn_reason` ON `contract_sets_churn`(`reason`);
CREATE INDEX `idx_contract_sets_churn_direction` ON `contract_sets_churn`(`direction`);
CREATE INDEX `idx_contract_sets_churn_fc_id` ON `contract_sets_churn`(`fc_id`);
CREATE INDEX `idx_contract_sets_churn_name` ON `contract_sets_churn`(`name`);
CREATE INDEX `idx_contract_sets_churn_timestamp` ON `contract_sets_churn`(`timestamp`);

-- dbPerformanceMetric
CREATE TABLE `performance` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`action` text NOT NULL,`host` blob NOT NULL,`origin` text NOT NULL,`duration` integer NOT NULL);
CREATE INDEX `idx_performance_duration` ON `performance`(`duration`);
CREATE INDEX `idx_performance_origin` ON `performance`(`origin`);
CREATE INDEX `idx_performance_host` ON `performance`(`host`);
CREATE INDEX `idx_performance_action` ON `performance`(`action`);
CREATE INDEX `idx_performance_timestamp` ON `performance`(`timestamp`);

-- dbWalletMetric
CREATE TABLE `wallets` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`timestamp` BIGINT NOT NULL,`confirmed_lo` BIGINT NOT NULL,`confirmed_hi` BIGINT NOT NULL,`spendable_lo` BIGINT NOT NULL,`spendable_hi` BIGINT NOT NULL,`unconfirmed_lo` BIGINT NOT NULL,`unconfirmed_hi` BIGINT NOT NULL);
CREATE INDEX `idx_unconfirmed` ON `wallets`(`unconfirmed_lo`,`unconfirmed_hi`);
CREATE INDEX `idx_spendable` ON `wallets`(`spendable_lo`,`spendable_hi`);
CREATE INDEX `idx_confirmed` ON `wallets`(`confirmed_lo`,`confirmed_hi`);
CREATE INDEX `idx_wallets_timestamp` ON `wallets`(`timestamp`);
