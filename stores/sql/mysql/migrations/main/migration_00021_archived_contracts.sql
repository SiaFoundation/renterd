ALTER TABLE contracts
    RENAME COLUMN total_cost TO initial_renter_funds,
    RENAME COLUMN list_spending TO sector_roots_spending;

ALTER TABLE contracts
    ADD COLUMN host_key varbinary(32) NOT NULL DEFAULT UNHEX('0000000000000000000000000000000000000000000000000000000000000000'),
    ADD COLUMN archival_reason VARCHAR(191) DEFAULT NULL,
    ADD COLUMN renewed_to VARBINARY(32) DEFAULT NULL;

CREATE INDEX `idx_contracts_archival_reason` ON `contracts`(`archival_reason`);
CREATE INDEX `idx_contracts_host_key` ON `contracts`(`host_key`);
CREATE INDEX `idx_contracts_renewed_to` ON `contracts`(`renewed_to`);

DROP INDEX `idx_contracts_fc_id` ON table_name;
CREATE INDEX `idx_contracts_fc_id` ON `contracts`(`fcid`);

UPDATE contracts c
INNER JOIN hosts h ON c.host_id = h.id
SET c.host_key = h.public_key;

INSERT INTO contracts (
    created_at,
    fcid,
    host_id,
    host_key,
    archival_reason,
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
    ac.fcid,
    h.id,
    COALESCE(h.public_key, UNHEX('0000000000000000000000000000000000000000000000000000000000000000')),
    ac.reason,
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

DROP TABLE `archived_contracts`;