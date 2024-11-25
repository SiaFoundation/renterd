-- drop default contract set from settings
UPDATE settings SET value=JSON_REMOVE(value, '$.defaultContractSet') WHERE `key`="upload";

-- remove contract set reference from slabs table
ALTER TABLE slabs DROP FOREIGN KEY fk_slabs_db_contract_set;
ALTER TABLE slabs DROP COLUMN db_contract_set_id;

-- update usability
UPDATE contracts SET usability = CASE WHEN EXISTS (SELECT 1 FROM contract_set_contracts WHERE db_contract_id = contracts.id) THEN 2 ELSE 1 END;

-- drop contract set tables
DROP TABLE contract_set_contracts;
DROP TABLE contract_sets;
