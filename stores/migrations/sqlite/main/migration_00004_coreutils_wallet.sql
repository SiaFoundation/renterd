-- drop tables
DROP TABLE IF EXISTS `siacoin_elements`;
DROP TABLE IF EXISTS `transactions`;
-- TODO: DROP TABLE IF EXISTS `consensus_infos`;

-- dbWalletEvent
CREATE TABLE `wallet_events` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`event_id` blob NOT NULL,`inflow` text,`outflow` text,`transaction` text,`maturity_height` integer,`source` text,`timestamp` integer,`height` integer, `block_id` blob);
CREATE UNIQUE INDEX `idx_wallet_events_event_id` ON `wallet_events`(`event_id`);
CREATE INDEX `idx_wallet_events_maturity_height` ON `wallet_events`(`maturity_height`);
CREATE INDEX `idx_wallet_events_source` ON `wallet_events`(`source`);
CREATE INDEX `idx_wallet_events_timestamp` ON `wallet_events`(`timestamp`);
CREATE INDEX `idx_wallet_events_height` ON `wallet_events`(`height`);

-- dbWalletOutput
CREATE TABLE `wallet_outputs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`output_id` blob NOT NULL,`leaf_index` integer,`merkle_proof` blob NOT NULL,`value` text,`address` blob,`maturity_height` integer,`height` integer, `block_id` blob);
CREATE UNIQUE INDEX `idx_wallet_outputs_output_id` ON `wallet_outputs`(`output_id`);
CREATE INDEX `idx_wallet_outputs_maturity_height` ON `wallet_outputs`(`maturity_height`);
CREATE INDEX `idx_wallet_outputs_height` ON `wallet_outputs`(`height`);