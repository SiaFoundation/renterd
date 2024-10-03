UPDATE settings
SET value = (
	-- Update settings to new values
	SELECT JSON_REPLACE(value, '$.maxDownloadPrice', newMaxDownloadPrice, '$.maxUploadPrice', newMaxUploadPrice)
	FROM (
		-- Convert TB to bytes by trimming the last 12 digits
		SELECT
			SUBSTR(maxDownloadPrice, 1, LENGTH(maxDownloadPrice)-12) AS newMaxDownloadPrice,
			SUBSTR(maxUploadPrice, 1, LENGTH(maxUploadPrice)-12) AS newMaxUploadPrice
		FROM (
			-- SELECT previous settings
			SELECT
				JSON_UNQUOTE(JSON_EXTRACT(value, '$.maxDownloadPrice')) AS maxDownloadPrice,
				JSON_UNQUOTE(JSON_EXTRACT(value, '$.maxUploadPrice')) AS maxUploadPrice
		) AS _
	) AS _
)
WHERE settings.key = "gouging";
