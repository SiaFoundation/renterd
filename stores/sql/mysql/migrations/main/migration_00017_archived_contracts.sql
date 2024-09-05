/*
TODOs:
- drop list_spending
- drop download_spending
- rename total_cost
*/

DROP TABLE IF EXISTS contracts_temp;

CREATE TABLE contracts_temp (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,

  `archival_reason` varchar(191) NOT NULL DEFAULT '',
  `fcid` varbinary(32) NOT NULL,
  `host_key` varbinary(32) NOT NULL,
  `proof_height` bigint unsigned DEFAULT '0',
  `renewed_from` varbinary(32) DEFAULT NULL,
  `renewed_to` varbinary(32) DEFAULT NULL,
  `revision_height` bigint unsigned DEFAULT '0',
  `revision_number` varchar(191) NOT NULL DEFAULT '0',
  `size` bigint unsigned DEFAULT NULL,
  `start_height` bigint unsigned NOT NULL,
  `state` tinyint unsigned NOT NULL DEFAULT '0',
  `window_start` bigint unsigned NOT NULL DEFAULT '0',
  `window_end` bigint unsigned NOT NULL DEFAULT '0',

  `contract_price` longtext,
  `total_cost` longtext,

  `delete_spending` longtext,
  `download_spending` longtext,
  `fund_account_spending` longtext,
  `list_spending` longtext,
  `upload_spending` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fcid` (`fcid`),
  KEY `idx_contracts_archival_reason` (`archival_reason`),
  KEY `idx_contracts_fcid` (`fcid`),
  KEY `idx_contracts_host_key` (`host_key`),
  KEY `idx_contracts_proof_height` (`proof_height`),
  KEY `idx_contracts_renewed_from` (`renewed_from`),
  KEY `idx_contracts_renewed_to` (`renewed_to`),
  KEY `idx_contracts_revision_height` (`revision_height`),
  KEY `idx_contracts_start_height` (`start_height`),
  KEY `idx_contracts_state` (`state`),
  KEY `idx_contracts_window_start` (`window_start`),
  KEY `idx_contracts_window_end` (`window_end`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
    total_cost,
    delete_spending,
    download_spending,
    fund_account_spending,
    list_spending,
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
    c.download_spending,
    c.fund_account_spending,
    c.list_spending,
    c.upload_spending
FROM contracts c
INNER JOIN hosts h ON c.host_id = h.id;

INSERT INTO contracts_temp (
    created_at,
    fcid,
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
    total_cost,
    delete_spending,
    download_spending,
    fund_account_spending,
    list_spending,
    upload_spending
) SELECT
    ac.created_at,
    ac.fcid,
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
    ac.download_spending,
    ac.fund_account_spending,
    ac.list_spending,
    ac.upload_spending
FROM `archived_contracts` ac
LEFT JOIN hosts h ON ac.host = h.public_key;

DROP TABLE `contracts`;
DROP TABLE `archived_contracts`;

ALTER TABLE contracts_temp RENAME TO contracts;
