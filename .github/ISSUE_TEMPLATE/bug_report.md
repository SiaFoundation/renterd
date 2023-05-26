---
name: Bug report
about: Create a report to help us improve `renterd`
title: ''
labels: ''
assignees: ''

---

## Describe the bug
A clear and concise description of what the bug is.

## Expected behaviour
A clear and concise description of what you expected to happen.

## Additional context

### General Information
Are you running a fork of `renterd`?
Are you on the mainnet or on the testnet?

### Renterd Config
Please provide us the following information:

**Autopilot Config**

```bash
curl -u ":[YOUR_PASSWORD]" http://localhost:9980/api/autopilot/config
```

**Bus Config**

```bash
curl -u ":[YOUR_PASSWORD]" http://localhost:9980/api/bus/setting/redundancy

curl -u ":[YOUR_PASSWORD]" http://localhost:9980/api/bus/setting/gouging
```

**Contract Set Contracts**

```bash
curl -u ":[YOUR_PASSWORD]" http://localhost:9980/api/bus/contracts/set/autopilot | grep '"id"' | wc -l
```

### Renterd Logs
If applicable, upload your `renterd.log` file or part of the file you think is relevant to the issue.
