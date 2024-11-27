CREATE TABLE `contract_elements` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime,
    `db_contract_id` integer NOT NULL,
    `contract` blob NOT NULL,
    `leaf_index` integer,
    `merkle_proof` longblob NOT NULL,
    CONSTRAINT `fk_contract_elements_contracts` FOREIGN KEY (`db_contract_id`) REFERENCES `contracts`(`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_contract_elements_db_contract_id` ON `contract_elements`(`db_contract_id`);
