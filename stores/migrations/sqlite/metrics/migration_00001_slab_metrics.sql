-- dbSlabMetric
CREATE TABLE `slabs` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, `created_at` DATETIME, `timestamp` BIGINT NOT NULL, `action` INTEGER NOT NULL CHECK (`action` >= 0 AND `action` <= 255), `speed_bytes_per_ms` BIGINT NOT NULL, `min_shards` INTEGER NOT NULL CHECK (`min_shards` >= 0 AND `min_shards` <= 255), `total_shards` INTEGER NOT NULL CHECK (`total_shards` >= 0 AND `total_shards` <= 255), `num_migrated` INTEGER NOT NULL CHECK (`num_migrated` >= 0 AND `num_migrated` <= 255), `num_overdrive` BIGINT NOT NULL);
CREATE INDEX `idx_slab_metric_action` ON `slabs` (`action`);
CREATE INDEX `idx_slab_metric_speed` ON `slabs` (`speed_bytes_per_ms`);
CREATE INDEX `idx_slab_metric_min_shards` ON `slabs` (`min_shards`);
CREATE INDEX `idx_slab_metric_total_shards` ON `slabs` (`total_shards`);
CREATE INDEX `idx_slab_metric_num_migrated` ON `slabs` (`num_migrated`);
CREATE INDEX `idx_slab_metric_num_overdrive` ON `slabs` (`num_overdrive`);
