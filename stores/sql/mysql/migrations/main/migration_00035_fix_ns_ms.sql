-- update gouging setting durations from ns to ms
UPDATE settings
SET
    value = (
        -- Update settings to new values
        SELECT
            JSON_REPLACE (
                value,
                '$.minAccountExpiry',
                newMinAccountExpiry,
                '$.minPriceTableValidity',
                newMinPriceTableValidity
            )
        FROM
            (
                -- Convert μs to ms by trimming the last 3 digits
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
                            JSON_UNQUOTE (JSON_EXTRACT (value, '$.minAccountExpiry')) AS minAccountExpiry,
                            JSON_UNQUOTE (JSON_EXTRACT (value, '$.minPriceTableValidity')) AS minPriceTableValidity
                    ) AS _
            ) AS _
    )
WHERE
    settings.key = "gouging";
