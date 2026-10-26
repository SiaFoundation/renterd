---
default: patch
---

# Validate the host's latest revision before pruning

The bus now verifies that the revision number didn't decrease and that the fields of a contract which can't change over its lifetime (keys, proof and expiration heights, output addresses, total collateral and payout) match the contract it knows about before signing a derived revision for pruning. Also updated `coreutils` to v0.25.2 which verifies the renter signature on fetched revisions.
