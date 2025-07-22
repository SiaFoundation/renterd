DROP TABLE `wallet_locked_outputs`;
CREATE TABLE wallet_broadcasted_txnsets (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `block_id` varbinary(32) NOT NULL,
  `height` bigint unsigned NOT NULL,
  `txn_set_id` varbinary(32) NOT NULL,
  `raw_transactions` longblob NOT NULL, -- binary serialized transaction set
  PRIMARY KEY (`id`),
  UNIQUE KEY wallet_broadcasted_txnsets_txn_set_id (`txn_set_id`),
  KEY wallet_broadcasted_txnsets_created_at (`created_at`)
);
