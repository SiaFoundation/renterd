-- add v2 settings to host
ALTER TABLE hosts
ADD COLUMN v2_settings text;

UPDATE hosts
SET
    v2_settings = '{}';

-- drop resolved addresses
ALTER TABLE hosts
DROP COLUMN resolved_addresses;

-- add column to host_checks
ALTER TABLE host_checks
ADD COLUMN usability_low_max_duration INTEGER NOT NULL DEFAULT 0;

CREATE INDEX `idx_host_checks_usability_low_max_duration` ON `host_checks` (`usability_low_max_duration`);

-- drop host announcements
DROP TABLE host_announcements;

-- add new table host_addresses
CREATE TABLE `host_addresses` (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `created_at` datetime NOT NULL,
    `db_host_id` integer NOT NULL,
    `net_address` text NOT NULL,
    `protocol` integer NOT NULL,
    CONSTRAINT `fk_host_addresses_db_host` FOREIGN KEY (`db_host_id`) REFERENCES `hosts` (`id`) ON DELETE CASCADE
);

CREATE INDEX `idx_host_addresses_db_host_id` ON `host_addresses` (`db_host_id`);

-- update gouging setting durations from ns to ms
UPDATE settings
SET
    value = (
        -- Update settings to new values
        SELECT
            JSON_REPLACE (
                value,
                '$.minAccountExpiry',
                CAST(newMinAccountExpiry AS INTEGER),
                '$.minPriceTableValidity',
                CAST(newMinPriceTableValidity AS INTEGER)
            )
        FROM
            (
                -- Convert ns to ms by trimming the last 3 digits
                SELECT
                    SUBSTR(minAccountExpiry, 1, LENGTH (minAccountExpiry) -3) AS newMinAccountExpiry,
                    SUBSTR(
                        minPriceTableValidity,
                        1,
                        LENGTH (minPriceTableValidity) -3
                    ) AS newMinPriceTableValidity
                FROM
                    (
                        -- SELECT previous settings
                        SELECT
                            JSON_EXTRACT (value, '$.minAccountExpiry') AS minAccountExpiry,
                            JSON_EXTRACT (value, '$.minPriceTableValidity') AS minPriceTableValidity
                    ) AS _
            ) AS _
    )
WHERE
    settings.key = "gouging";
