DROP TABLE IF EXISTS contracts_temp;

CREATE TABLE contracts_temp (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime,
    `fcid` blob NOT NULL UNIQUE,
    `host_key` blob NOT NULL,

    `archival_reason` text NOT NULL DEFAULT "",
    `proof_height` integer DEFAULT 0,
    `renewed_from` blob,
    `renewed_to` blob,
    `revision_height` integer DEFAULT 0,
    `revision_number` text NOT NULL DEFAULT "0",
    `size` integer,
    `start_height` integer NOT NULL,
    `state` integer NOT NULL DEFAULT 0,
    `window_start` integer NOT NULL DEFAULT 0,
    `window_end` integer NOT NULL DEFAULT 0,

    `contract_price` text,
    `initial_renter_funds` text,

    `delete_spending` text,
    `sector_roots_spending` text,
    `fund_account_spending` text,
    `upload_spending` text
);
CREATE INDEX `idx_contracts_archival_reason` ON `contracts_temp`(`archival_reason`);
CREATE INDEX `idx_contracts_fcid` ON `contracts_temp`(`fcid`);
CREATE INDEX `idx_contracts_host_key` ON `contracts_temp`(`host_key`);
CREATE INDEX `idx_contracts_proof_height` ON `contracts_temp`(`proof_height`);
CREATE INDEX `idx_contracts_renewed_from` ON `contracts_temp`(`renewed_from`);
CREATE INDEX `idx_contracts_renewed_to` ON `contracts_temp`(`renewed_to`);
CREATE INDEX `idx_contracts_revision_height` ON `contracts_temp`(`revision_height`);
CREATE INDEX `idx_contracts_start_height` ON `contracts_temp`(`start_height`);
CREATE INDEX `idx_contracts_state` ON `contracts_temp`(`state`);
CREATE INDEX `idx_contracts_window_end` ON `contracts_temp`(`window_end`);
CREATE INDEX `idx_contracts_window_start` ON `contracts_temp`(`window_start`);


INSERT INTO contracts_temp (
    id,
    created_at,
    fcid,
    host_key,
    proof_height,
    renewed_from,
    revision_height,
    revision_number,
    size,
    start_height,
    state,
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
    h.public_key,
    c.proof_height,
    c.renewed_from,
    c.revision_height,
    c.revision_number,
    c.size,
    c.start_height,
    c.state,
    c.window_start,
    c.window_end,
    c.contract_price,
    c.total_cost,
    c.delete_spending,
    c.fund_account_spending,
    c.list_spending,
    c.upload_spending
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id;

INSERT INTO contracts_temp (
    created_at,
    archival_reason,
    fcid,
    host_key,
    proof_height,
    renewed_from,
    renewed_to,
    revision_height,
    revision_number,
    size,
    start_height,
    state,
    window_start,
    window_end,
    contract_price,
    initial_renter_funds,
    delete_spending,
    fund_account_spending,
    sector_roots_spending,
    upload_spending
) SELECT
    ac.created_at,
    ac.reason,
    ac.fcid,
    COALESCE(h.public_key, X'0000000000000000000000000000000000000000000000000000000000000000'),
    ac.proof_height,
    ac.renewed_from,
    ac.renewed_to,
    ac.revision_height,
    ac.revision_number,
    ac.size,
    ac.start_height,
    ac.state,
    ac.window_start,
    ac.window_end,
    ac.contract_price,
    ac.total_cost,
    ac.delete_spending,
    ac.fund_account_spending,
    ac.list_spending,
    ac.upload_spending
FROM `archived_contracts` ac
LEFT JOIN hosts h ON ac.host = h.public_key;

ALTER TABLE contract_sectors DROP FOREIGN KEY fk_contract_sectors_db_contract;
ALTER TABLE contract_set_contracts DROP FOREIGN KEY fk_contract_set_contracts_db_contract;

DROP TABLE `contracts`;
DROP TABLE `archived_contracts`;

ALTER TABLE contracts_temp RENAME TO contracts;
ALTER TABLE contract_sectors ADD CONSTRAINT fk_contract_sectors_db_contract FOREIGN KEY (db_contract_id) REFERENCES contracts(id) ON DELETE CASCADE;
ALTER TABLE contract_set_contracts ADD CONSTRAINT fk_contract_set_contracts_db_contract FOREIGN KEY (db_contract_id) REFERENCES contracts(id) ON DELETE CASCADE;