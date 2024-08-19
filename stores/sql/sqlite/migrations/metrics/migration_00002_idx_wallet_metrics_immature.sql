ALTER TABLE `wallets` ADD COLUMN `immature_lo` BIGINT NOT NULL, ADD COLUMN `immature_hi` BIGINT NOT NULL;
CREATE INDEX `idx_wallets_immature` ON `wallets`(`immature_lo`,`immature_hi`);