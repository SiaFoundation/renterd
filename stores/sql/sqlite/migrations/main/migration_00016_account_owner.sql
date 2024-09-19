DROP TABLE IF EXISTS ephemeral_accounts;

CREATE TABLE `ephemeral_accounts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`account_id` blob NOT NULL UNIQUE,`clean_shutdown` numeric DEFAULT false,`host` blob NOT NULL,`balance` text,`drift` text,`requires_sync` numeric, `owner` text NOT NULL);
CREATE INDEX `idx_ephemeral_accounts_requires_sync` ON `ephemeral_accounts`(`requires_sync`);
CREATE INDEX `idx_ephemeral_accounts_owner` ON `ephemeral_accounts`(`owner`);