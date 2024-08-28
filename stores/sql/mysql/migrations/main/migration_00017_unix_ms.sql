UPDATE hosts SET hosts.last_scan = hosts.last_scan / 1000;
UPDATE wallet_events SET wallet_events.timestamp = wallet_events.timestamp / 1000;
