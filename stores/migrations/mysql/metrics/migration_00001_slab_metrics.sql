-- dbSlabMetric
CREATE TABLE `slabs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3) DEFAULT NULL,
  `timestamp` bigint NOT NULL,
  `action` TINYINT UNSIGNED NOT NULL,
  `speed_bytes_per_ms` bigint NOT NULL,
  `min_shards` TINYINT UNSIGNED NOT NULL,
  `total_shards` TINYINT UNSIGNED NOT NULL,
  `num_migrated` TINYINT UNSIGNED NOT NULL,
  `num_overdrive` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_slab_metric_action` (`action`),
  KEY `idx_slab_metric_speed` (`speed_bytes_per_ms`),
  KEY `idx_slab_metric_min_shards` (`min_shards`),
  KEY `idx_slab_metric_total_shards` (`total_shards`),
  KEY `idx_slab_metric_num_migrated` (`num_migrated`),
  KEY `idx_slab_metric_num_overdrive` (`num_overdrive`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;