---
default: minor
---

# Allow for bypassing basic auth using a 'renterd_auth' cookie

Added a new `POST /auth` endpoint with a single required parameter 'validity'
(ms) which creates a new renterd auth token. The client can set that token as
the value of the 'renterd_auth' cookie to bypass basic auth for the duration of
the token's validity.
