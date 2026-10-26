---
default: patch
---

# Fix autopilot panic on hosts with extreme prices

A host announcing prices that overflow the contract funding calculation no longer crashes the autopilot during contract formation, refresh or renewal. Hosts that fail to be scored are now marked unusable instead of keeping the result of their previous check.
