CREATE TABLE `contracts_temp` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime,
    `timestamp` BIGINT NOT NULL,
    `fcid` blob NOT NULL,
    `host` blob NOT NULL,
    `remaining_collateral_lo` BIGINT NOT NULL,
    `remaining_collateral_hi` BIGINT NOT NULL,
    `remaining_funds_lo` BIGINT NOT NULL,
    `remaining_funds_hi` BIGINT NOT NULL,
    `revision_number` BIGINT NOT NULL,
    `upload_spending_lo` BIGINT NOT NULL,
    `upload_spending_hi` BIGINT NOT NULL,
    `fund_account_spending_lo` BIGINT NOT NULL,
    `fund_account_spending_hi` BIGINT NOT NULL,
    `delete_spending_lo` BIGINT NOT NULL,
    `delete_spending_hi` BIGINT NOT NULL,
    `sector_roots_spending_lo` BIGINT NOT NULL,
    `sector_roots_spending_hi` BIGINT NOT NULL
);
CREATE INDEX `idx_sector_roots_spending` ON `contracts_temp`(`sector_roots_spending_lo`,`sector_roots_spending_hi`);
CREATE INDEX `idx_fund_account_spending` ON `contracts_temp`(`fund_account_spending_lo`,`fund_account_spending_hi`);
CREATE INDEX `idx_contracts_fc_id` ON `contracts_temp`(`fcid`);
CREATE INDEX `idx_remaining_collateral` ON `contracts_temp`(`remaining_collateral_lo`,`remaining_collateral_hi`);
CREATE INDEX `idx_contracts_host` ON `contracts_temp`(`host`);
CREATE INDEX `idx_contracts_timestamp` ON `contracts_temp`(`timestamp`);
CREATE INDEX `idx_delete_spending` ON `contracts_temp`(`delete_spending_lo`,`delete_spending_hi`);
CREATE INDEX `idx_upload_spending` ON `contracts_temp`(`upload_spending_lo`,`upload_spending_hi`);
CREATE INDEX `idx_contracts_revision_number` ON `contracts_temp`(`revision_number`);
CREATE INDEX `idx_remaining_funds` ON `contracts_temp`(`remaining_funds_lo`,`remaining_funds_hi`);
CREATE INDEX `idx_contracts_fcid_timestamp` ON `contracts_temp`(`fcid`,`timestamp`);

INSERT INTO contracts_temp (
    id,
    created_at,
    timestamp,
    fcid,
    host,
    remaining_collateral_lo,
    remaining_collateral_hi,
    remaining_funds_lo,
    remaining_funds_hi,
    revision_number,
    upload_spending_lo,
    upload_spending_hi,
    fund_account_spending_lo,
    fund_account_spending_hi,
    delete_spending_lo,
    delete_spending_hi,
    sector_roots_spending_lo,
    sector_roots_spending_hi
) SELECT
    id,
    created_at,
    timestamp,
    fcid,
    host,
    remaining_collateral_lo,
    remaining_collateral_hi,
    remaining_funds_lo,
    remaining_funds_hi,
    revision_number,
    upload_spending_lo,
    upload_spending_hi,
    fund_account_spending_lo,
    fund_account_spending_hi,
    delete_spending_lo,
    delete_spending_hi,
    list_spending_lo,
    list_spending_hi
FROM contracts;

DROP TABLE `contracts`;
ALTER TABLE contracts_temp RENAME TO contracts;
