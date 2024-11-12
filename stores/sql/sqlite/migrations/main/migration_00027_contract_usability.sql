-- create a temporary table that adds the usability column
DROP TABLE IF EXISTS contracts_temp;
CREATE TABLE contracts_temp (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime,
    `fcid` blob NOT NULL UNIQUE,
    `host_id` integer,
    `host_key` blob NOT NULL,

    `v2` INTEGER NOT NULL,
    `archival_reason` text DEFAULT NULL,
    `proof_height` integer DEFAULT 0,
    `renewed_from` blob,
    `renewed_to` blob,
    `revision_height` integer DEFAULT 0,
    `revision_number` text NOT NULL DEFAULT "0",
    `size` integer,
    `start_height` integer NOT NULL,
    `state` integer NOT NULL DEFAULT 0,
    `usability` integer NOT NULL,
    `window_start` integer NOT NULL DEFAULT 0,
    `window_end` integer NOT NULL DEFAULT 0,

    `contract_price` text,
    `initial_renter_funds` text,

    `delete_spending` text,
    `fund_account_spending` text,
    `sector_roots_spending` text,
    `upload_spending` text,

    CONSTRAINT `fk_contracts_host` FOREIGN KEY (`host_id`) REFERENCES `hosts`(`id`)
);

DROP INDEX IF EXISTS `idx_contracts_archival_reason`;
DROP INDEX IF EXISTS `idx_contracts_fcid`;
DROP INDEX IF EXISTS `idx_contracts_host_id`;
DROP INDEX IF EXISTS `idx_contracts_host_key`;
DROP INDEX IF EXISTS `idx_contracts_proof_height`;
DROP INDEX IF EXISTS `idx_contracts_renewed_from`;
DROP INDEX IF EXISTS `idx_contracts_renewed_to`;
DROP INDEX IF EXISTS `idx_contracts_revision_height`;
DROP INDEX IF EXISTS `idx_contracts_start_height`;
DROP INDEX IF EXISTS `idx_contracts_state`;
DROP INDEX IF EXISTS `idx_contracts_window_end`;
DROP INDEX IF EXISTS `idx_contracts_window_start`;

CREATE INDEX `idx_contracts_archival_reason` ON `contracts_temp`(`archival_reason`);
CREATE INDEX `idx_contracts_fcid` ON `contracts_temp`(`fcid`);
CREATE INDEX `idx_contracts_host_id` ON `contracts_temp`(`host_id`);
CREATE INDEX `idx_contracts_host_key` ON `contracts_temp`(`host_key`);
CREATE INDEX `idx_contracts_proof_height` ON `contracts_temp`(`proof_height`);
CREATE INDEX `idx_contracts_renewed_from` ON `contracts_temp`(`renewed_from`);
CREATE INDEX `idx_contracts_renewed_to` ON `contracts_temp`(`renewed_to`);
CREATE INDEX `idx_contracts_revision_height` ON `contracts_temp`(`revision_height`);
CREATE INDEX `idx_contracts_start_height` ON `contracts_temp`(`start_height`);
CREATE INDEX `idx_contracts_state` ON `contracts_temp`(`state`);
CREATE INDEX `idx_contracts_usability` ON `contracts_temp`(`usability`);
CREATE INDEX `idx_contracts_window_end` ON `contracts_temp`(`window_end`);
CREATE INDEX `idx_contracts_window_start` ON `contracts_temp`(`window_start`);

INSERT INTO contracts_temp (
    id,
    created_at,
    fcid,
    host_id,
    host_key,
    v2,
    archival_reason,
    proof_height,
    renewed_from,
    renewed_to,
    revision_height,
    revision_number,
    size,
    start_height,
    state,
    usability,
    window_start,
    window_end,
    contract_price,
    initial_renter_funds,
    delete_spending,
    fund_account_spending,
    sector_roots_spending,
    upload_spending
) SELECT
    c.id,
    c.created_at,
    c.fcid,
    c.host_id,
    c.host_key,
    c.v2,
    c.archival_reason,
    c.proof_height,
    c.renewed_from,
    c.renewed_to,
    c.revision_height,
    c.revision_number,
    c.size,
    c.start_height,
    c.state,
    CASE WHEN archival_reason IS NULL THEN 2 ELSE 1 END,
    c.window_start,
    c.window_end,
    c.contract_price,
    c.initial_renter_funds,
    c.delete_spending,
    c.fund_account_spending,
    c.sector_roots_spending,
    c.upload_spending
FROM contracts c;

DROP TABLE `contracts`;
ALTER TABLE contracts_temp RENAME TO contracts;