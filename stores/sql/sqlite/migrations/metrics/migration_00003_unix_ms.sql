UPDATE contract_prunes SET timestamp = CAST(timestamp / 1000000 AS SIGNED);
DROP TABLE IF EXISTS performance;
