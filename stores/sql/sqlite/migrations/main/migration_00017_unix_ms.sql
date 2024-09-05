UPDATE hosts SET last_scan = CAST(last_scan / 1000000 AS SIGNED);
UPDATE hosts SET uptime = CAST(uptime / 1000000 AS SIGNED);
UPDATE hosts SET downtime = CAST(downtime / 1000000 AS SIGNED);
UPDATE wallet_events SET timestamp = CAST(timestamp / 1000000 AS SIGNED);
