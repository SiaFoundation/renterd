-- drop tables
DROP TABLE IF EXISTS `siacoin_elements`;
DROP TABLE IF EXISTS `transactions`;

-- drop column
ALTER TABLE `consensus_infos` DROP COLUMN `cc_id`;

-- dbWalletEvent
CREATE TABLE `wallet_events` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`event_id` blob NOT NULL,`inflow` text,`outflow` text,`transaction` text,`maturity_height` integer,`source` text,`timestamp` integer,`db_chain_index_id` integer,CONSTRAINT `fk_wallet_events_chain_index` FOREIGN KEY (`db_chain_index_id`) REFERENCES `chain_indices`(`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_wallet_events_event_id` ON `wallet_events`(`event_id`);
CREATE INDEX `idx_wallet_events_maturity_height` ON `wallet_events`(`maturity_height`);
CREATE INDEX `idx_wallet_events_source` ON `wallet_events`(`source`);
CREATE INDEX `idx_wallet_events_timestamp` ON `wallet_events`(`timestamp`);

-- dbWalletOutput
CREATE TABLE `wallet_outputs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`output_id` blob NOT NULL,`leaf_index` integer,`merkle_proof` blob NOT NULL,`value` text,`address` blob,`maturity_height` integer,`db_chain_index_id` integer,CONSTRAINT `fk_contract_set_contracts_db_contract` FOREIGN KEY (`db_chain_index_id`) REFERENCES `chain_indices`(`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_wallet_outputs_output_id` ON `wallet_outputs`(`output_id`);
CREATE INDEX `idx_wallet_outputs_maturity_height` ON `wallet_outputs`(`maturity_height`);

-- dbChainIndex
CREATE TABLE `chain_indices` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`height` integer, `block_id` blob);
CREATE UNIQUE INDEX `idx_chain_indices_height` ON `chain_indices`(`height`);
CREATE UNIQUE INDEX `idx_chain_indices_block_id` ON `chain_indices`(`block_id`);