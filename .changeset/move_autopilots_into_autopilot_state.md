---
default: minor
---

# Move `autopilots` into `autopilot_state`

#1657 by @peterjan

This migrates the `autopilots` to a single `autopilot_state` table, effectively removing the concept of multiple autopilots. I think autopilot state can be defined as config + the current period, I opted to keep the `AutopilotConfig` which can be updated in the `bus`. The `current_period` can be updated separately, which solves a race in the autopilot but that won't be an issue anyway since we're merging autopilot and bus. Tested on SQLite, not (yet) on MySQL, want to get a round of reviews in first.
