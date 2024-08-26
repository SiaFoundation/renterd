ALTER TABLE `wallets` ADD COLUMN `immature_lo` BIGINT DEFAULT 0;
ALTER TABLE `wallets` ADD COLUMN `immature_hi` BIGINT DEFAULT 0;
CREATE INDEX `idx_wallets_immature` ON `wallets`(`immature_lo`,`immature_hi`);