DROP TABLE IF EXISTS ephemeral_accounts;

CREATE TABLE `ephemeral_accounts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `account_id` varbinary(32) NOT NULL,
  `clean_shutdown` tinyint(1) DEFAULT '0',
  `host` longblob NOT NULL,
  `balance` longtext,
  `drift` longtext,
  `requires_sync` tinyint(1) DEFAULT NULL,
  `owner` varchar(128) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `account_id` (`account_id`),
  KEY `idx_ephemeral_accounts_requires_sync` (`requires_sync`),
  KEY `idx_ephemeral_accounts_owner` (`owner`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;