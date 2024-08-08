#!/bin/sh

if [[ "$RENTERD_NETWORK" == *'mainnet'* ]]; then
    exec renterd -env -http=':9980' -s3.address=':8080' "$@"
else
    exec renterd -env -http=':9880' -s3.address=':7070' "$@"
fi
