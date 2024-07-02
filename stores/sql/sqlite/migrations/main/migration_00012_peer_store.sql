-- dbSyncerPeer
CREATE TABLE `syncer_peers` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`address` text NOT NULL,`first_seen` BIGINT NOT NULL,`last_connect` BIGINT,`synced_blocks` BIGINT,`sync_duration` BIGINT);
CREATE UNIQUE INDEX `idx_syncer_peers_address` ON `syncer_peers`(`address`);

-- dbSyncerBan
CREATE TABLE `syncer_bans` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`net_cidr` text  NOT NULL,`reason` text,`expiration` BIGINT NOT NULL);
CREATE UNIQUE INDEX `idx_syncer_bans_net_cidr` ON `syncer_bans`(`net_cidr`);