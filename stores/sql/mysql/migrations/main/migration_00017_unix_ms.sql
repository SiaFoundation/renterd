UPDATE hosts SET hosts.last_scan = CAST(hosts.last_scan / 1000000 AS SIGNED);
UPDATE hosts SET hosts.uptime = CAST(hosts.uptime / 1000000 AS SIGNED);
UPDATE hosts SET hosts.downtime = CAST(hosts.downtime / 1000000 AS SIGNED);
UPDATE wallet_events SET wallet_events.timestamp = CAST(wallet_events.timestamp / 1000000 AS SIGNED);
