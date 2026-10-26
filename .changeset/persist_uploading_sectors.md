---
default: patch
---

# Persist uploading sectors in the bus database

Sectors of in-flight uploads are now tracked in the database instead of in memory so a bus restart mid-upload no longer allows the pruner to delete sectors of uploads that are still in progress.
