CREATE TABLE `wallet_locked_outputs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime, `output_id` blob NOT NULL, `unlock_timestamp` integer NOT NULL);
CREATE UNIQUE INDEX `idx_wallet_locked_outputs_output_id` ON `wallet_locked_outputs`(`output_id`);
CREATE INDEX `idx_wallet_locked_outputs_unlock_timestamp` ON `wallet_locked_outputs`(`unlock_timestamp`);
