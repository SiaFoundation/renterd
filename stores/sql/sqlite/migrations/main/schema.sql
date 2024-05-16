-- dbArchivedContract
CREATE TABLE `archived_contracts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`fcid` blob NOT NULL UNIQUE,`renewed_from` blob,`contract_price` text,`state` integer NOT NULL DEFAULT 0,`total_cost` text,`proof_height` integer DEFAULT 0,`revision_height` integer DEFAULT 0,`revision_number` text NOT NULL DEFAULT "0",`size` integer,`start_height` integer NOT NULL,`window_start` integer NOT NULL DEFAULT 0,`window_end` integer NOT NULL DEFAULT 0,`upload_spending` text,`download_spending` text,`fund_account_spending` text,`delete_spending` text,`list_spending` text,`renewed_to` blob,`host` blob NOT NULL,`reason` text);
CREATE INDEX `idx_archived_contracts_start_height` ON `archived_contracts`(`start_height`);
CREATE INDEX `idx_archived_contracts_revision_height` ON `archived_contracts`(`revision_height`);
CREATE INDEX `idx_archived_contracts_proof_height` ON `archived_contracts`(`proof_height`);
CREATE INDEX `idx_archived_contracts_fc_id` ON `archived_contracts`(`fcid`);
CREATE INDEX `idx_archived_contracts_host` ON `archived_contracts`(`host`);
CREATE INDEX `idx_archived_contracts_renewed_to` ON `archived_contracts`(`renewed_to`);
CREATE INDEX `idx_archived_contracts_window_end` ON `archived_contracts`(`window_end`);
CREATE INDEX `idx_archived_contracts_window_start` ON `archived_contracts`(`window_start`);
CREATE INDEX `idx_archived_contracts_state` ON `archived_contracts`(`state`);
CREATE INDEX `idx_archived_contracts_renewed_from` ON `archived_contracts`(`renewed_from`);

-- dbHost
CREATE TABLE `hosts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`public_key` blob NOT NULL UNIQUE,`settings` text,`price_table` text,`price_table_expiry` datetime,`total_scans` integer,`last_scan` integer,`last_scan_success` numeric,`second_to_last_scan_success` numeric,`scanned` numeric,`uptime` integer,`downtime` integer,`recent_downtime` integer,`recent_scan_failures` integer,`successful_interactions` real,`failed_interactions` real,`lost_sectors` integer,`last_announcement` datetime,`net_address` text);
CREATE INDEX `idx_hosts_recent_scan_failures` ON `hosts`(`recent_scan_failures`);
CREATE INDEX `idx_hosts_recent_downtime` ON `hosts`(`recent_downtime`);
CREATE INDEX `idx_hosts_scanned` ON `hosts`(`scanned`);
CREATE INDEX `idx_hosts_last_scan` ON `hosts`(`last_scan`);
CREATE INDEX `idx_hosts_public_key` ON `hosts`(`public_key`);
CREATE INDEX `idx_hosts_net_address` ON `hosts`(`net_address`);

-- dbContract
CREATE TABLE `contracts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`fcid` blob NOT NULL UNIQUE,`renewed_from` blob,`contract_price` text,`state` integer NOT NULL DEFAULT 0,`total_cost` text,`proof_height` integer DEFAULT 0,`revision_height` integer DEFAULT 0,`revision_number` text NOT NULL DEFAULT "0",`size` integer,`start_height` integer NOT NULL,`window_start` integer NOT NULL DEFAULT 0,`window_end` integer NOT NULL DEFAULT 0,`upload_spending` text,`download_spending` text,`fund_account_spending` text,`delete_spending` text,`list_spending` text,`host_id` integer,CONSTRAINT `fk_contracts_host` FOREIGN KEY (`host_id`) REFERENCES `hosts`(`id`));
CREATE INDEX `idx_contracts_proof_height` ON `contracts`(`proof_height`);
CREATE INDEX `idx_contracts_state` ON `contracts`(`state`);
CREATE INDEX `idx_contracts_renewed_from` ON `contracts`(`renewed_from`);
CREATE INDEX `idx_contracts_host_id` ON `contracts`(`host_id`);
CREATE INDEX `idx_contracts_window_end` ON `contracts`(`window_end`);
CREATE INDEX `idx_contracts_window_start` ON `contracts`(`window_start`);
CREATE INDEX `idx_contracts_revision_height` ON `contracts`(`revision_height`);
CREATE INDEX `idx_contracts_start_height` ON `contracts`(`start_height`);
CREATE INDEX `idx_contracts_fc_id` ON `contracts`(`fcid`);

-- dbContractSet
CREATE TABLE `contract_sets` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`name` text UNIQUE);
CREATE INDEX `idx_contract_sets_name` ON `contract_sets`(`name`);

-- dbContractSet <-> dbContract
CREATE TABLE `contract_set_contracts` (`db_contract_set_id` integer,`db_contract_id` integer,PRIMARY KEY (`db_contract_set_id`,`db_contract_id`),CONSTRAINT `fk_contract_set_contracts_db_contract_set` FOREIGN KEY (`db_contract_set_id`) REFERENCES `contract_sets`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_contract_set_contracts_db_contract` FOREIGN KEY (`db_contract_id`) REFERENCES `contracts`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_contract_set_contracts_db_contract_id` ON `contract_set_contracts`(`db_contract_id`);

-- dbBucket
CREATE TABLE `buckets` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`policy` text,`name` text NOT NULL UNIQUE);
CREATE INDEX `idx_buckets_name` ON `buckets`(`name`);

-- dbObject
CREATE TABLE `objects` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_bucket_id` integer NOT NULL,`object_id` text,`key` blob,`health` real NOT NULL DEFAULT 1,`size` integer,`mime_type` text,`etag` text,CONSTRAINT `fk_objects_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets`(`id`));
CREATE INDEX `idx_objects_db_bucket_id` ON `objects`(`db_bucket_id`);
CREATE INDEX `idx_objects_etag` ON `objects`(`etag`);
CREATE INDEX `idx_objects_health` ON `objects`(`health`);
CREATE INDEX `idx_objects_object_id` ON `objects`(`object_id`);
CREATE INDEX `idx_objects_size` ON `objects`(`size`);
CREATE UNIQUE INDEX `idx_object_bucket` ON `objects`(`db_bucket_id`,`object_id`);
CREATE INDEX `idx_objects_created_at` ON `objects`(`created_at`);

-- dbMultipartUpload
CREATE TABLE `multipart_uploads` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`key` blob,`upload_id` text NOT NULL,`object_id` text NOT NULL,`db_bucket_id` integer NOT NULL,`mime_type` text,CONSTRAINT `fk_multipart_uploads_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_multipart_uploads_mime_type` ON `multipart_uploads`(`mime_type`);
CREATE INDEX `idx_multipart_uploads_db_bucket_id` ON `multipart_uploads`(`db_bucket_id`);
CREATE INDEX `idx_multipart_uploads_object_id` ON `multipart_uploads`(`object_id`);
CREATE UNIQUE INDEX `idx_multipart_uploads_upload_id` ON `multipart_uploads`(`upload_id`);

-- dbBufferedSlab
CREATE TABLE `buffered_slabs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`filename` text);

-- dbSlab
CREATE TABLE `slabs` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_contract_set_id` integer,`db_buffered_slab_id` integer DEFAULT NULL,`health` real NOT NULL DEFAULT 1,`health_valid_until` integer NOT NULL DEFAULT 0,`key` blob NOT NULL UNIQUE,`min_shards` integer,`total_shards` integer,CONSTRAINT `fk_buffered_slabs_db_slab` FOREIGN KEY (`db_buffered_slab_id`) REFERENCES `buffered_slabs`(`id`),CONSTRAINT `fk_slabs_db_contract_set` FOREIGN KEY (`db_contract_set_id`) REFERENCES `contract_sets`(`id`));
CREATE INDEX `idx_slabs_db_contract_set_id` ON `slabs`(`db_contract_set_id`);
CREATE INDEX `idx_slabs_total_shards` ON `slabs`(`total_shards`);
CREATE INDEX `idx_slabs_min_shards` ON `slabs`(`min_shards`);
CREATE INDEX `idx_slabs_health_valid_until` ON `slabs`(`health_valid_until`);
CREATE INDEX `idx_slabs_health` ON `slabs`(`health`);
CREATE INDEX `idx_slabs_db_buffered_slab_id` ON `slabs`(`db_buffered_slab_id`);

-- dbSector
CREATE TABLE `sectors` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_slab_id` integer NOT NULL,`slab_index` integer NOT NULL,`latest_host` blob NOT NULL,`root` blob NOT NULL UNIQUE,CONSTRAINT `fk_slabs_shards` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_sectors_slab_index` ON `sectors`(`slab_index`);
CREATE UNIQUE INDEX `idx_sectors_slab_id_slab_index` ON `sectors`(`db_slab_id`,`slab_index`);
CREATE INDEX `idx_sectors_db_slab_id` ON `sectors`(`db_slab_id`);
CREATE INDEX `idx_sectors_root` ON `sectors`(`root`);

-- dbContract <-> dbSector
CREATE TABLE `contract_sectors` (`db_sector_id` integer,`db_contract_id` integer,PRIMARY KEY (`db_sector_id`,`db_contract_id`),CONSTRAINT `fk_contract_sectors_db_sector` FOREIGN KEY (`db_sector_id`) REFERENCES `sectors`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_contract_sectors_db_contract` FOREIGN KEY (`db_contract_id`) REFERENCES `contracts`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_contract_sectors_db_contract_id` ON `contract_sectors`(`db_contract_id`);
CREATE INDEX `idx_contract_sectors_db_sector_id` ON `contract_sectors`(`db_sector_id`);

-- dbMultipartPart
CREATE TABLE `multipart_parts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`etag` text,`part_number` integer,`size` integer,`db_multipart_upload_id` integer NOT NULL,CONSTRAINT `fk_multipart_uploads_parts` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_multipart_parts_db_multipart_upload_id` ON `multipart_parts`(`db_multipart_upload_id`);
CREATE INDEX `idx_multipart_parts_part_number` ON `multipart_parts`(`part_number`);
CREATE INDEX `idx_multipart_parts_etag` ON `multipart_parts`(`etag`);

-- dbSlice
CREATE TABLE `slices` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer,`object_index` integer,`db_multipart_part_id` integer,`db_slab_id` integer,`offset` integer,`length` integer,CONSTRAINT `fk_objects_slabs` FOREIGN KEY (`db_object_id`) REFERENCES `objects`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_multipart_parts_slabs` FOREIGN KEY (`db_multipart_part_id`) REFERENCES `multipart_parts`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_slabs_slices` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs`(`id`));
CREATE INDEX `idx_slices_object_index` ON `slices`(`object_index`);
CREATE INDEX `idx_slices_db_object_id` ON `slices`(`db_object_id`);
CREATE INDEX `idx_slices_db_slab_id` ON `slices`(`db_slab_id`);
CREATE INDEX `idx_slices_db_multipart_part_id` ON `slices`(`db_multipart_part_id`);

-- dbHostAnnouncement
CREATE TABLE `host_announcements` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`host_key` blob NOT NULL,`block_height` integer,`block_id` text,`net_address` text);

-- dbConsensusInfo
CREATE TABLE `consensus_infos` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`cc_id` blob,`height` integer,`block_id` blob);

-- dbBlocklistEntry
CREATE TABLE `host_blocklist_entries` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`entry` text NOT NULL UNIQUE);
CREATE INDEX `idx_host_blocklist_entries_entry` ON `host_blocklist_entries`(`entry`);

-- dbBlocklistEntry <-> dbHost
CREATE TABLE `host_blocklist_entry_hosts` (`db_blocklist_entry_id` integer,`db_host_id` integer,PRIMARY KEY (`db_blocklist_entry_id`,`db_host_id`),CONSTRAINT `fk_host_blocklist_entry_hosts_db_blocklist_entry` FOREIGN KEY (`db_blocklist_entry_id`) REFERENCES `host_blocklist_entries`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_host_blocklist_entry_hosts_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_host_blocklist_entry_hosts_db_host_id` ON `host_blocklist_entry_hosts`(`db_host_id`);

-- dbAllowlistEntry
CREATE TABLE `host_allowlist_entries` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`entry` blob NOT NULL UNIQUE);
CREATE INDEX `idx_host_allowlist_entries_entry` ON `host_allowlist_entries`(`entry`);

-- dbAllowlistEntry <-> dbHost
CREATE TABLE `host_allowlist_entry_hosts` (`db_allowlist_entry_id` integer,`db_host_id` integer,PRIMARY KEY (`db_allowlist_entry_id`,`db_host_id`),CONSTRAINT `fk_host_allowlist_entry_hosts_db_allowlist_entry` FOREIGN KEY (`db_allowlist_entry_id`) REFERENCES `host_allowlist_entries`(`id`) ON DELETE CASCADE,CONSTRAINT `fk_host_allowlist_entry_hosts_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts`(`id`) ON DELETE CASCADE);
CREATE INDEX `idx_host_allowlist_entry_hosts_db_host_id` ON `host_allowlist_entry_hosts`(`db_host_id`);

-- dbSiacoinElement
CREATE TABLE `siacoin_elements` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`value` text,`address` blob,`output_id` blob NOT NULL UNIQUE,`maturity_height` integer);
CREATE INDEX `idx_siacoin_elements_maturity_height` ON `siacoin_elements`(`maturity_height`);
CREATE INDEX `idx_siacoin_elements_output_id` ON `siacoin_elements`(`output_id`);

-- dbTransaction
CREATE TABLE `transactions` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`raw` text,`height` integer,`block_id` blob,`transaction_id` blob NOT NULL UNIQUE,`inflow` text,`outflow` text,`timestamp` integer);
CREATE INDEX `idx_transactions_timestamp` ON `transactions`(`timestamp`);
CREATE INDEX `idx_transactions_transaction_id` ON `transactions`(`transaction_id`);

-- dbSetting
CREATE TABLE `settings` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`key` text NOT NULL UNIQUE,`value` text NOT NULL);
CREATE INDEX `idx_settings_key` ON `settings`(`key`);

-- dbAccount
CREATE TABLE `ephemeral_accounts` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`account_id` blob NOT NULL UNIQUE,`clean_shutdown` numeric DEFAULT false,`host` blob NOT NULL,`balance` text,`drift` text,`requires_sync` numeric);
CREATE INDEX `idx_ephemeral_accounts_requires_sync` ON `ephemeral_accounts`(`requires_sync`);

-- dbAutopilot
CREATE TABLE `autopilots` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`identifier` text NOT NULL UNIQUE,`config` text,`current_period` integer DEFAULT 0);

-- dbWebhook
CREATE TABLE `webhooks` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`module` text NOT NULL,`event` text NOT NULL,`url` text NOT NULL);
CREATE UNIQUE INDEX `idx_module_event_url` ON `webhooks`(`module`,`event`,`url`);

-- dbObjectUserMetadata
CREATE TABLE `object_user_metadata` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`db_object_id` integer DEFAULT NULL,`db_multipart_upload_id` integer DEFAULT NULL,`key` text NOT NULL,`value` text, CONSTRAINT `fk_object_user_metadata` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE, CONSTRAINT `fk_multipart_upload_user_metadata` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE SET NULL);
CREATE UNIQUE INDEX `idx_object_user_metadata_key` ON `object_user_metadata`(`db_object_id`,`db_multipart_upload_id`,`key`);

-- dbHostCheck
CREATE TABLE `host_checks` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `created_at` datetime, `db_autopilot_id` INTEGER NOT NULL, `db_host_id` INTEGER NOT NULL, `usability_blocked` INTEGER NOT NULL DEFAULT 0, `usability_offline` INTEGER NOT NULL DEFAULT 0, `usability_low_score` INTEGER NOT NULL DEFAULT 0, `usability_redundant_ip` INTEGER NOT NULL DEFAULT 0, `usability_gouging` INTEGER NOT NULL DEFAULT 0, `usability_not_accepting_contracts` INTEGER NOT NULL DEFAULT 0, `usability_not_announced` INTEGER NOT NULL DEFAULT 0, `usability_not_completing_scan` INTEGER NOT NULL DEFAULT 0, `score_age` REAL NOT NULL, `score_collateral` REAL NOT NULL, `score_interactions` REAL NOT NULL, `score_storage_remaining` REAL NOT NULL, `score_uptime` REAL NOT NULL, `score_version` REAL NOT NULL, `score_prices` REAL NOT NULL, `gouging_contract_err` TEXT, `gouging_download_err` TEXT, `gouging_gouging_err` TEXT, `gouging_prune_err` TEXT, `gouging_upload_err` TEXT, FOREIGN KEY (`db_autopilot_id`) REFERENCES `autopilots` (`id`) ON DELETE CASCADE, FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE);
CREATE UNIQUE INDEX `idx_host_checks_id` ON `host_checks` (`db_autopilot_id`, `db_host_id`);
CREATE INDEX `idx_host_checks_usability_blocked` ON `host_checks` (`usability_blocked`);
CREATE INDEX `idx_host_checks_usability_offline` ON `host_checks` (`usability_offline`);
CREATE INDEX `idx_host_checks_usability_low_score` ON `host_checks` (`usability_low_score`);
CREATE INDEX `idx_host_checks_usability_redundant_ip` ON `host_checks` (`usability_redundant_ip`);
CREATE INDEX `idx_host_checks_usability_gouging` ON `host_checks` (`usability_gouging`);
CREATE INDEX `idx_host_checks_usability_not_accepting_contracts` ON `host_checks` (`usability_not_accepting_contracts`);
CREATE INDEX `idx_host_checks_usability_not_announced` ON `host_checks` (`usability_not_announced`);
CREATE INDEX `idx_host_checks_usability_not_completing_scan` ON `host_checks` (`usability_not_completing_scan`);
CREATE INDEX `idx_host_checks_score_age` ON `host_checks` (`score_age`);
CREATE INDEX `idx_host_checks_score_collateral` ON `host_checks` (`score_collateral`);
CREATE INDEX `idx_host_checks_score_interactions` ON `host_checks` (`score_interactions`);
CREATE INDEX `idx_host_checks_score_storage_remaining` ON `host_checks` (`score_storage_remaining`);
CREATE INDEX `idx_host_checks_score_uptime` ON `host_checks` (`score_uptime`);
CREATE INDEX `idx_host_checks_score_version` ON `host_checks` (`score_version`);
CREATE INDEX `idx_host_checks_score_prices` ON `host_checks` (`score_prices`);

-- create default bucket
INSERT INTO buckets (created_at, name) VALUES (CURRENT_TIMESTAMP, 'default');
