UPDATE hosts SET hosts.last_scan = hosts.last_scan / 1000000;
UPDATE hosts SET hosts.uptime = hosts.uptime / 1000000;
UPDATE hosts SET hosts.downtime = hosts.downtime / 1000000;
UPDATE wallet_events SET wallet_events.timestamp = wallet_events.timestamp / 1000000;
