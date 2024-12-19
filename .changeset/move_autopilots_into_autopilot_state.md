---
default: major
---

# Move `autopilots` into `autopilot_state`

#1657 by @peterjan

This migrates the `autopilots` to a single `autopilot_state` table, effectively removing the concept of multiple autopilots.