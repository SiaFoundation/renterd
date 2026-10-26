---
default: patch
---

# Fix worker panic on range requests with extreme offsets or lengths

Range requests whose offset and length overflow are rejected with an invalid range error instead of crashing the worker.
