-- add v2 settings to host
ALTER TABLE hosts
ADD COLUMN v2_settings JSON;

UPDATE hosts
SET
    hosts.v2_settings = '{}';

-- drop resolved addresses
ALTER TABLE hosts
DROP COLUMN resolved_addresses;

-- add column to host_checks
ALTER TABLE host_checks
ADD COLUMN `usability_low_max_duration` boolean NOT NULL DEFAULT false;

CREATE INDEX `idx_host_checks_usability_low_max_duration` ON `host_checks` (`usability_low_max_duration`);

-- drop host announcements
DROP TABLE host_announcements;

-- add new table host_addresses
CREATE TABLE `host_addresses` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `created_at` datetime (3) DEFAULT NULL,
    `db_host_id` bigint unsigned NOT NULL,
    `net_address` longtext NOT NULL,
    `protocol` tinyint unsigned NOT NULL,
    PRIMARY KEY (`id`),
    KEY `ìdx_host_addresses_db_host_id` (`db_host_id`),
    CONSTRAINT `fk_host_addresses_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
