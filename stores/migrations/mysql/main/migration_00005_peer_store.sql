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
