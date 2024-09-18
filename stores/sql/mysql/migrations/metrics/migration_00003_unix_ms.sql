UPDATE contract_prunes SET contract_prunes.timestamp = CAST(contract_prunes.timestamp / 1000000 AS SIGNED);
DROP TABLE IF EXISTS performance;
