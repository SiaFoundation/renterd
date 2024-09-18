-- avoid duplicate key errors
DELETE FROM settings WHERE `key` IN ("s3", "upload", "pinned");

-- migrate settings
INSERT INTO settings (created_at, `key`, value)
SELECT NOW(), k, v
FROM (
    -- upload is a combination of uploadpacking, redundancy, and contractset
    SELECT
        "upload" as k,
        json_merge_patch(
            json_object("packing", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "uploadpacking")),
            json_merge_patch(
                json_object("redundancy", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "redundancy")),
                json_object("defaultContractSet", (SELECT json_extract(value, "$.default") FROM settings WHERE `key` = "contractset"))
            )
        ) as v
    WHERE json_extract(
            json_merge_patch(
                json_object("packing", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "uploadpacking")),
                json_merge_patch(
                    json_object("redundancy", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "redundancy")),
                    json_object("defaultContractSet", (SELECT json_extract(value, "$.default") FROM settings WHERE `key` = "contractset"))
                )
            ), "$.packing"
        ) IS NOT NULL
      AND json_extract(
            json_merge_patch(
                json_object("packing", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "uploadpacking")),
                json_merge_patch(
                    json_object("redundancy", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "redundancy")),
                    json_object("defaultContractSet", (SELECT json_extract(value, "$.default") FROM settings WHERE `key` = "contractset"))
                )
            ), "$.redundancy"
        ) IS NOT NULL

    UNION ALL

    -- s3 wraps the s3authentication setting
    SELECT
        "s3" as k,
        json_object("authentication", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "s3authentication")) as v
    WHERE json_extract(
            json_object("authentication", (SELECT json_extract(value, "$") FROM settings WHERE `key` = "s3authentication")),
            "$.authentication"
        ) IS NOT NULL

    UNION ALL

    -- pinning renames pricepinning and removes the 'enabled' and 'forexEndpointURL' fields
    SELECT
        "pinned" as k,
        json_remove(
            json_remove(
                (SELECT json_extract(value, "$") FROM settings WHERE `key` = "pricepinning"),
                "$.enabled"
            ),
            "$.forexEndpointURL"
        ) as v
    WHERE json_extract(
            json_remove(
                json_remove(
                    (SELECT json_extract(value, "$") FROM settings WHERE `key` = "pricepinning"),
                    "$.enabled"
                ),
                "$.forexEndpointURL"
            ),
            "$.currency"
        ) IS NOT NULL
      AND json_extract(
            json_remove(
                json_remove(
                    (SELECT json_extract(value, "$") FROM settings WHERE `key` = "pricepinning"),
                    "$.enabled"
                ),
                "$.forexEndpointURL"
            ),
            "$.threshold"
        ) IS NOT NULL
) as migration;

-- delete old settings
DELETE FROM settings WHERE `key` IN ("uploadpacking", "redundancy", "contractset", "s3authentication", "pricepinning");
