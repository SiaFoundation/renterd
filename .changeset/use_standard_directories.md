---
default: major
---

# Use standard locations for application data

 Uses standard locations for application data instead of the current directory. This brings `renterd` in line with other system services and makes it easier to manage application data.

 #### Linux, FreeBSD, OpenBSD
 - Configuration: `/etc/renterd/renterd.yml`
 - Data directory: `/var/lib/renterd`

 #### macOS
 - Configuration: `~/Library/Application Support/renterd.yml`
 - Data directory: `~/Library/Application Support/renterd`

 #### Windows
 - Configuration: `%APPDATA%\SiaFoundation\renterd.yml`
 - Data directory: `%APPDATA%\SiaFoundation\renterd`

 #### Docker
 - Configuration: `/data/renterd.yml`
 - Data directory: `/data`
