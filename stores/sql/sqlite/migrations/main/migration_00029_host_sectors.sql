INSERT OR IGNORE INTO host_sectors (db_sector_id, db_host_id)
SELECT cs.db_sector_id, c.host_id
FROM contract_sectors cs
INNER JOIN contracts c ON cs.db_contract_id = c.id AND c.host_id IS NOT NULL