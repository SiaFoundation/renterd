---
default: minor
---

# Add openapi.yml file with autopilot routes

Added an openapi.yml spec with the specifications for the autopilot routes and a CI step to validate it. The goal is to eventually have a complete spec for the V2 API that we can use to generate API docs as well as making sure that there is always a valid spec for every given commit in the repo.
