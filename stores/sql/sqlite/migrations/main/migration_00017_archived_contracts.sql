ALTER TABLE `contracts` ADD COLUMN archival_reason text NOT NULL DEFAULT '';
CREATE INDEX `idx_contracts_archival_reason` ON `contracts`(`archival_reason`);

ALTER TABLE `contracts` ADD COLUMN renewed_to blob;
CREATE INDEX `idx_contracts_renewed_to` ON `contracts`(`renewed_to`);

INSERT INTO `contracts` (
created_at,
fcid,
renewed_from,
renewed_to,
contract_price,
state,
total_cost,
proof_height,
revision_height,
revision_number,
size,
start_height,
window_start,
window_end,
upload_spending,
download_spending,
fund_account_spending,
delete_spending,
list_spending,
host_id,
archival_reason
) SELECT
ac.created_at,
ac.fcid,
ac.renewed_from,
ac.renewed_to,
ac.contract_price,
ac.state,
ac.total_cost,
ac.proof_height,
ac.revision_height,
ac.revision_number,
ac.size,
ac.start_height,
ac.window_start,
ac.window_end,
ac.upload_spending,
ac.download_spending,
ac.fund_account_spending,
ac.delete_spending,
ac.list_spending,
h.id,
ac.reason
FROM `archived_contracts` ac INNER JOIN hosts h ON ac.host = h.public_key ;
DROP TABLE `archived_contracts`;
