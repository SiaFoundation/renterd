-- drop tables
DROP TABLE IF EXISTS `siacoin_elements`;
DROP TABLE IF EXISTS `transactions`;

-- drop column
ALTER TABLE `consensus_infos` DROP COLUMN `cc_id`;

-- dbWalletEvent
CREATE TABLE `wallet_events` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`event_id` blob NOT NULL,`height` integer, `block_id` blob,`inflow` text,`outflow` text,`type` text NOT NULL,`data` longblob NOT NULL,`maturity_height` integer,`timestamp` integer);
CREATE UNIQUE INDEX `idx_wallet_events_event_id` ON `wallet_events`(`event_id`);
CREATE INDEX `idx_wallet_events_maturity_height` ON `wallet_events`(`maturity_height`);
CREATE INDEX `idx_wallet_events_type` ON `wallet_events`(`type`);
CREATE INDEX `idx_wallet_events_timestamp` ON `wallet_events`(`timestamp`);
CREATE INDEX `idx_wallet_events_block_id_height` ON `wallet_events`(`block_id`,`height`);

-- dbWalletOutput
CREATE TABLE `wallet_outputs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`output_id` blob NOT NULL,`leaf_index` integer,`merkle_proof` longblob NOT NULL,`value` text,`address` blob,`maturity_height` integer);
CREATE UNIQUE INDEX `idx_wallet_outputs_output_id` ON `wallet_outputs`(`output_id`);
CREATE INDEX `idx_wallet_outputs_maturity_height` ON `wallet_outputs`(`maturity_height`);