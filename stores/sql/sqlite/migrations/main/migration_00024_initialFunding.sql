UPDATE autopilots SET config = JSON_SET(autopilots.config, '$.contracts.initialFunding', '10000000000000000000000000');
