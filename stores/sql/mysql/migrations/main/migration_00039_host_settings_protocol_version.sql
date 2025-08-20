UPDATE hosts
SET v2_settings = JSON_REPLACE(v2_settings, '$.protocolVersion', "v4.0.0")
WHERE v2_settings != '{}';
