UPDATE contract_prunes SET contract_prunes.timestamp = contract_prunes.timestamp / 1000000;
DROP TABLE IF EXISTS performance;
