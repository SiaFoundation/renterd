-- dbBucket
CREATE TABLE `buckets` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `policy` JSON,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `idx_buckets_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbBufferedSlab
CREATE TABLE `buffered_slabs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `filename` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbConsensusInfo
CREATE TABLE `consensus_infos` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `height` bigint unsigned DEFAULT NULL,
  `block_id` longblob,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbHost
CREATE TABLE `hosts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `public_key` varbinary(32) NOT NULL,
  `settings` JSON,
  `price_table` longtext,
  `price_table_expiry` datetime(3) DEFAULT NULL,
  `total_scans` bigint unsigned DEFAULT NULL,
  `last_scan` bigint DEFAULT NULL,
  `last_scan_success` tinyint(1) DEFAULT NULL,
  `second_to_last_scan_success` tinyint(1) DEFAULT NULL,
  `scanned` tinyint(1) DEFAULT NULL,
  `uptime` bigint DEFAULT NULL,
  `downtime` bigint DEFAULT NULL,
  `recent_downtime` bigint DEFAULT NULL,
  `recent_scan_failures` bigint unsigned DEFAULT NULL,
  `successful_interactions` double DEFAULT NULL,
  `failed_interactions` double DEFAULT NULL,
  `lost_sectors` bigint unsigned DEFAULT NULL,
  `last_announcement` datetime(3) DEFAULT NULL,
  `net_address` varchar(191) DEFAULT NULL,
  `resolved_addresses` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `public_key` (`public_key`),
  KEY `idx_hosts_public_key` (`public_key`),
  KEY `idx_hosts_last_scan` (`last_scan`),
  KEY `idx_hosts_scanned` (`scanned`),
  KEY `idx_hosts_recent_downtime` (`recent_downtime`),
  KEY `idx_hosts_recent_scan_failures` (`recent_scan_failures`),
  KEY `idx_hosts_net_address` (`net_address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbContract
CREATE TABLE `contracts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `fcid` varbinary(32) NOT NULL,
  `host_id` bigint unsigned DEFAULT NULL,
  `host_key` varbinary(32),
  `v2` boolean NOT NULL,

  `archival_reason` varchar(191) DEFAULT NULL,
  `proof_height` bigint unsigned DEFAULT '0',
  `renewed_from` varbinary(32) DEFAULT NULL,
  `renewed_to` varbinary(32) DEFAULT NULL,
  `revision_height` bigint unsigned DEFAULT '0',
  `revision_number` varchar(191) NOT NULL DEFAULT '0',
  `size` bigint unsigned DEFAULT NULL,
  `start_height` bigint unsigned NOT NULL,
  `state` tinyint unsigned NOT NULL DEFAULT '0',
  `usability` tinyint unsigned NOT NULL,
  `window_start` bigint unsigned NOT NULL DEFAULT '0',
  `window_end` bigint unsigned NOT NULL DEFAULT '0',

  `contract_price` longtext,
  `initial_renter_funds` longtext,

  `delete_spending` longtext,
  `fund_account_spending` longtext,
  `sector_roots_spending` longtext,
  `upload_spending` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fcid` (`fcid`),
  KEY `idx_contracts_archival_reason` (`archival_reason`),
  KEY `idx_contracts_fcid` (`fcid`),
  KEY `idx_contracts_host_id` (`host_id`),
  KEY `idx_contracts_host_key` (`host_key`),
  KEY `idx_contracts_proof_height` (`proof_height`),
  KEY `idx_contracts_renewed_from` (`renewed_from`),
  KEY `idx_contracts_renewed_to` (`renewed_to`),
  KEY `idx_contracts_revision_height` (`revision_height`),
  KEY `idx_contracts_start_height` (`start_height`),
  KEY `idx_contracts_state` (`state`),
  KEY `idx_contracts_usability` (`usability`),
  KEY `idx_contracts_window_start` (`window_start`),
  KEY `idx_contracts_window_end` (`window_end`),
  CONSTRAINT `fk_contracts_host` FOREIGN KEY (`host_id`) REFERENCES `hosts` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSlab
CREATE TABLE `slabs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_buffered_slab_id` bigint unsigned DEFAULT NULL,
  `health` double NOT NULL DEFAULT '1',
  `health_valid_until` bigint NOT NULL DEFAULT '0',
  `key` binary(33) NOT NULL,
  `min_shards` tinyint unsigned DEFAULT NULL,
  `total_shards` tinyint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  KEY `idx_slabs_min_shards` (`min_shards`),
  KEY `idx_slabs_total_shards` (`total_shards`),
  KEY `idx_slabs_db_buffered_slab_id` (`db_buffered_slab_id`),
  KEY `idx_slabs_health` (`health`),
  KEY `idx_slabs_health_valid_until` (`health_valid_until`),
  CONSTRAINT `fk_buffered_slabs_db_slab` FOREIGN KEY (`db_buffered_slab_id`) REFERENCES `buffered_slabs` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSector
CREATE TABLE `sectors` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_slab_id` bigint unsigned NOT NULL,
  `slab_index` bigint NOT NULL,
  `root` varbinary(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `root` (`root`),
  UNIQUE KEY `idx_sectors_slab_id_slab_index` (`db_slab_id`,`slab_index`),
  KEY `idx_sectors_db_slab_id` (`db_slab_id`),
  KEY `idx_sectors_slab_index` (`slab_index`),
  KEY `idx_sectors_root` (`root`),
  CONSTRAINT `fk_slabs_shards` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbContract <-> dbSector
CREATE TABLE `contract_sectors` (
  `db_sector_id` bigint unsigned NOT NULL,
  `db_contract_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`db_sector_id`,`db_contract_id`),
  KEY `idx_contract_sectors_db_sector_id` (`db_sector_id`),
  KEY `idx_contract_sectors_db_contract_id` (`db_contract_id`),
  CONSTRAINT `fk_contract_sectors_db_contract` FOREIGN KEY (`db_contract_id`) REFERENCES `contracts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_contract_sectors_db_sector` FOREIGN KEY (`db_sector_id`) REFERENCES `sectors` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbAccount
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

-- dbAllowlistEntry
CREATE TABLE `host_allowlist_entries` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `entry` varbinary(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry` (`entry`),
  KEY `idx_host_allowlist_entries_entry` (`entry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbAllowlistEntry <-> dbHost
CREATE TABLE `host_allowlist_entry_hosts` (
  `db_allowlist_entry_id` bigint unsigned NOT NULL,
  `db_host_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`db_allowlist_entry_id`,`db_host_id`),
  KEY `idx_host_allowlist_entry_hosts_db_host_id` (`db_host_id`),
  CONSTRAINT `fk_host_allowlist_entry_hosts_db_allowlist_entry` FOREIGN KEY (`db_allowlist_entry_id`) REFERENCES `host_allowlist_entries` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_host_allowlist_entry_hosts_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbHostAnnouncement
CREATE TABLE `host_announcements` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `host_key` longblob NOT NULL,
  `block_height` bigint unsigned DEFAULT NULL,
  `block_id` longtext,
  `net_address` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbBlocklistEntry
CREATE TABLE `host_blocklist_entries` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `entry` varchar(191) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry` (`entry`),
  KEY `idx_host_blocklist_entries_entry` (`entry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbBlocklistEntry <-> dbHost
CREATE TABLE `host_blocklist_entry_hosts` (
  `db_blocklist_entry_id` bigint unsigned NOT NULL,
  `db_host_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`db_blocklist_entry_id`,`db_host_id`),
  KEY `idx_host_blocklist_entry_hosts_db_host_id` (`db_host_id`),
  CONSTRAINT `fk_host_blocklist_entry_hosts_db_blocklist_entry` FOREIGN KEY (`db_blocklist_entry_id`) REFERENCES `host_blocklist_entries` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_host_blocklist_entry_hosts_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbMultipartUpload
CREATE TABLE `multipart_uploads` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `key` binary(33) NOT NULL,
  `upload_id` varchar(64) NOT NULL,
  `object_id` varchar(766) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `db_bucket_id` bigint unsigned NOT NULL,
  `mime_type` varchar(191) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_multipart_uploads_upload_id` (`upload_id`),
  KEY `idx_multipart_uploads_object_id` (`object_id`),
  KEY `idx_multipart_uploads_db_bucket_id` (`db_bucket_id`),
  KEY `idx_multipart_uploads_mime_type` (`mime_type`),
  CONSTRAINT `fk_multipart_uploads_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbMultipartPart
CREATE TABLE `multipart_parts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `etag` varchar(191) DEFAULT NULL,
  `part_number` bigint DEFAULT NULL,
  `size` bigint unsigned DEFAULT NULL,
  `db_multipart_upload_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_multipart_parts_etag` (`etag`),
  KEY `idx_multipart_parts_part_number` (`part_number`),
  KEY `idx_multipart_parts_db_multipart_upload_id` (`db_multipart_upload_id`),
  CONSTRAINT `fk_multipart_uploads_parts` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbObject
CREATE TABLE `objects` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_bucket_id` bigint unsigned NOT NULL,
  `object_id` varchar(766) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `key` binary(33) NOT NULL,
  `health` double NOT NULL DEFAULT '1',
  `size` bigint DEFAULT NULL,
  `mime_type` longtext,
  `etag` varchar(191) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_object_bucket` (`db_bucket_id`,`object_id`),
  KEY `idx_objects_db_bucket_id` (`db_bucket_id`),
  KEY `idx_objects_object_id` (`object_id`),
  KEY `idx_objects_health` (`health`),
  KEY `idx_objects_etag` (`etag`),
  KEY `idx_objects_size` (`size`),
  KEY `idx_objects_created_at` (`created_at`),
  CONSTRAINT `fk_objects_db_bucket` FOREIGN KEY (`db_bucket_id`) REFERENCES `buckets` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSetting
CREATE TABLE `settings` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `key` varchar(191) NOT NULL,
  `value` longtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  KEY `idx_settings_key` (`key`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSlice
CREATE TABLE `slices` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_object_id` bigint unsigned DEFAULT NULL,
  `object_index` bigint unsigned DEFAULT NULL,
  `db_multipart_part_id` bigint unsigned DEFAULT NULL,
  `db_slab_id` bigint unsigned DEFAULT NULL,
  `offset` int unsigned DEFAULT NULL,
  `length` int unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_slices_db_object_id` (`db_object_id`),
  KEY `idx_slices_object_index` (`object_index`),
  KEY `idx_slices_db_multipart_part_id` (`db_multipart_part_id`),
  KEY `idx_slices_db_slab_id` (`db_slab_id`),
  CONSTRAINT `fk_multipart_parts_slabs` FOREIGN KEY (`db_multipart_part_id`) REFERENCES `multipart_parts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_objects_slabs` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_slabs_slices` FOREIGN KEY (`db_slab_id`) REFERENCES `slabs` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbWebhook
CREATE TABLE `webhooks` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `module` varchar(255) NOT NULL,
  `event` varchar(255) NOT NULL,
  `url` varchar(255) NOT NULL,
  `headers` JSON DEFAULT ('{}'),
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_module_event_url` (`module`,`event`,`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbObjectUserMetadata
CREATE TABLE `object_user_metadata` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `db_object_id` bigint unsigned DEFAULT NULL,
  `db_multipart_upload_id` bigint unsigned DEFAULT NULL,
  `key` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `value` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_object_user_metadata_key` (`db_object_id`, `db_multipart_upload_id`, `key`),
  CONSTRAINT `fk_object_user_metadata` FOREIGN KEY (`db_object_id`) REFERENCES `objects` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_multipart_upload_user_metadata` FOREIGN KEY (`db_multipart_upload_id`) REFERENCES `multipart_uploads` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbHostCheck
CREATE TABLE `host_checks` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,

  `db_host_id` bigint unsigned NOT NULL,

  `usability_blocked` boolean NOT NULL DEFAULT false,
  `usability_offline` boolean NOT NULL DEFAULT false,
  `usability_low_score` boolean NOT NULL DEFAULT false,
  `usability_redundant_ip` boolean NOT NULL DEFAULT false,
  `usability_gouging` boolean NOT NULL DEFAULT false,
  `usability_not_accepting_contracts` boolean NOT NULL DEFAULT false,
  `usability_not_announced` boolean NOT NULL DEFAULT false,
  `usability_not_completing_scan` boolean NOT NULL DEFAULT false,

  `score_age` double NOT NULL,
  `score_collateral` double NOT NULL,
  `score_interactions` double NOT NULL,
  `score_storage_remaining` double NOT NULL,
  `score_uptime` double NOT NULL,
  `score_version` double NOT NULL,
  `score_prices` double NOT NULL,

  `gouging_contract_err` text,
  `gouging_download_err` text,
  `gouging_gouging_err` text,
  `gouging_prune_err` text,
  `gouging_upload_err` text,

  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_host_checks_id` (`db_host_id`),
  INDEX `idx_host_checks_usability_blocked` (`usability_blocked`),
  INDEX `idx_host_checks_usability_offline` (`usability_offline`),
  INDEX `idx_host_checks_usability_low_score` (`usability_low_score`),
  INDEX `idx_host_checks_usability_redundant_ip` (`usability_redundant_ip`),
  INDEX `idx_host_checks_usability_gouging` (`usability_gouging`),
  INDEX `idx_host_checks_usability_not_accepting_contracts` (`usability_not_accepting_contracts`),
  INDEX `idx_host_checks_usability_not_announced` (`usability_not_announced`),
  INDEX `idx_host_checks_usability_not_completing_scan` (`usability_not_completing_scan`),
  INDEX `idx_host_checks_score_age` (`score_age`),
  INDEX `idx_host_checks_score_collateral` (`score_collateral`),
  INDEX `idx_host_checks_score_interactions` (`score_interactions`),
  INDEX `idx_host_checks_score_storage_remaining` (`score_storage_remaining`),
  INDEX `idx_host_checks_score_uptime` (`score_uptime`),
  INDEX `idx_host_checks_score_version` (`score_version`),
  INDEX `idx_host_checks_score_prices` (`score_prices`),

  CONSTRAINT `fk_host_checks_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSyncerPeer
CREATE TABLE `syncer_peers` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `address` varchar(191) NOT NULL,
  `first_seen` bigint NOT NULL,
  `last_connect` bigint,
  `synced_blocks` bigint,
  `sync_duration` bigint,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_syncer_peers_address` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbSyncerBan
CREATE TABLE `syncer_bans` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `net_cidr` varchar(191) NOT NULL,
  `reason` longtext,
  `expiration` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_syncer_bans_net_cidr` (`net_cidr`),
  KEY `idx_syncer_bans_expiration` (`expiration`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbWalletEvent
CREATE TABLE `wallet_events` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `event_id` varbinary(32) NOT NULL,
  `height` bigint unsigned DEFAULT NULL,
  `block_id` varbinary(32) NOT NULL,
  `inflow` longtext,
  `outflow` longtext,
  `type` varchar(191) NOT NULL,
  `data` longblob NOT NULL,
  `maturity_height` bigint unsigned DEFAULT NULL,
  `timestamp` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `event_id` (`event_id`),
  KEY `idx_wallet_events_maturity_height` (`maturity_height`),
  KEY `idx_wallet_events_type` (`type`),
  KEY `idx_wallet_events_timestamp` (`timestamp`),
  KEY `idx_wallet_events_block_id_height` (`block_id`, `height`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dbWalletOutput
CREATE TABLE `wallet_outputs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `output_id` varbinary(32) NOT NULL,
  `leaf_index` bigint,
  `merkle_proof` longblob NOT NULL,
  `value` longtext,
  `address` varbinary(32) DEFAULT NULL,
  `maturity_height` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `output_id` (`output_id`),
  KEY `idx_wallet_outputs_maturity_height` (`maturity_height`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `autopilot_config` (
  `id` bigint unsigned NOT NULL DEFAULT 1,
  `created_at` datetime(3) DEFAULT NULL,
  `enabled` boolean NOT NULL DEFAULT false,

  `contracts_amount` bigint unsigned DEFAULT NULL,
  `contracts_period` bigint unsigned DEFAULT NULL,
  `contracts_renew_window` bigint unsigned DEFAULT NULL,
  `contracts_download` bigint unsigned DEFAULT NULL,
  `contracts_upload` bigint unsigned DEFAULT NULL,
  `contracts_storage` bigint unsigned DEFAULT NULL,
  `contracts_prune` boolean NOT NULL DEFAULT false,

  `hosts_allow_redundant_ips` boolean NOT NULL DEFAULT false,
  `hosts_max_downtime_hours` bigint unsigned DEFAULT NULL,
  `hosts_min_protocol_version` varchar(191) DEFAULT NULL,
  `hosts_max_consecutive_scan_failures` bigint unsigned DEFAULT NULL,

  PRIMARY KEY (`id`),
  CHECK (`id` = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
