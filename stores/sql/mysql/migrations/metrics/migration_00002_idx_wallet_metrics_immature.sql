ALTER TABLE `wallets` ADD COLUMN `immature_lo` bigint NOT NULL, ADD COLUMN `immature_hi` bigint NOT NULL;
CREATE INDEX `idx_wallets_immature` ON `wallets`(`immature_lo`,`immature_hi`);
