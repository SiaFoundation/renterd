#!/bin/bash

if [[ "$BUILD_TAGS" == *'testnet'* ]]; then
    exec renterd -http=':9880' "$@"
else
    exec renterd -http=':9980' "$@"
fi