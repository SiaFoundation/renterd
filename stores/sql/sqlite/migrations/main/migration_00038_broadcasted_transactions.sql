DROP TABLE `wallet_locked_outputs`;
CREATE TABLE wallet_broadcasted_txnsets (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime NOT NULL,
    `block_id` blob NOT NULL,
    `height` integer NOT NULL,
    `txn_set_id` blob NOT NULL,
    `raw_transactions` BLOB NOT NULL -- binary serialized transaction set
);
CREATE UNIQUE INDEX wallet_broadcasted_txnsets_txn_set_id ON wallet_broadcasted_txnsets(txn_set_id);
CREATE INDEX wallet_broadcasted_txnsets_created_at ON wallet_broadcasted_txnsets(created_at);
