CREATE TABLE `contract_elements` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_contract_id` bigint unsigned NOT NULL,
  `contract` longblob NOT NULL,
  `leaf_index` bigint,
  `merkle_proof` longblob NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_contract_elements_db_contrat_id` (`db_contract_id`),
  CONSTRAINT `fk_contract_elements_contracts` FOREIGN KEY (`db_contract_id`) REFERENCES `contracts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
