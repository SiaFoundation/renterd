-- avoid duplicate key errors
DELETE FROM settings WHERE `key` IN ("s3", "upload", "pinned");

-- migrate settings
INSERT INTO settings (created_at, `key`, value)
SELECT NOW(), k, v
FROM (
    -- upload is a combination of uploadpacking, redundancy, and contractset
    SELECT
        "upload" as k,
        JSON_MERGE_PATCH(
            JSON_OBJECT("packing", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "uploadpacking")),
            JSON_MERGE_PATCH(
                JSON_OBJECT("redundancy", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "redundancy")),
                JSON_OBJECT("defaultContractSet", (SELECT JSON_EXTRACT(value, "$.default") FROM settings WHERE `key` = "contractset"))
            )
        ) as v
    WHERE JSON_EXTRACT(
            JSON_MERGE_PATCH(
                JSON_OBJECT("packing", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "uploadpacking")),
                JSON_MERGE_PATCH(
                    JSON_OBJECT("redundancy", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "redundancy")),
                    JSON_OBJECT("defaultContractSet", (SELECT JSON_EXTRACT(value, "$.default") FROM settings WHERE `key` = "contractset"))
                )
            ), "$.packing"
        ) IS NOT NULL
      AND JSON_EXTRACT(
            JSON_MERGE_PATCH(
                JSON_OBJECT("packing", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "uploadpacking")),
                JSON_MERGE_PATCH(
                    JSON_OBJECT("redundancy", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "redundancy")),
                    JSON_OBJECT("defaultContractSet", (SELECT JSON_EXTRACT(value, "$.default") FROM settings WHERE `key` = "contractset"))
                )
            ), "$.redundancy"
        ) IS NOT NULL

    UNION ALL

    -- s3 wraps the s3authentication setting
    SELECT
        "s3" as k,
        JSON_OBJECT("authentication", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "s3authentication")) as v
    WHERE JSON_EXTRACT(
            JSON_OBJECT("authentication", (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "s3authentication")),
            "$.authentication"
        ) IS NOT NULL

    UNION ALL

    -- pinning renames pricepinning and removes the 'enabled' and 'forexEndpointURL' fields
    SELECT
        "pinned" as k,
        JSON_REMOVE(
            JSON_REMOVE(
                (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "pricepinning"),
                "$.enabled"
            ),
            "$.forexEndpointURL"
        ) as v
    WHERE JSON_EXTRACT(
            JSON_REMOVE(
                JSON_REMOVE(
                    (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "pricepinning"),
                    "$.enabled"
                ),
                "$.forexEndpointURL"
            ),
            "$.currency"
        ) IS NOT NULL
      AND JSON_EXTRACT(
            JSON_REMOVE(
                JSON_REMOVE(
                    (SELECT JSON_EXTRACT(value, "$") FROM settings WHERE `key` = "pricepinning"),
                    "$.enabled"
                ),
                "$.forexEndpointURL"
            ),
            "$.threshold"
        ) IS NOT NULL
) as migration;

-- delete old settings (TODO: should we?)
DELETE FROM settings WHERE `key` IN ("uploadpacking", "redundancy", "contractset", "s3authentication", "pricepinning");
