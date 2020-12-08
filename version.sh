#!/bin/bash
set -e -o pipefail
echo -n "0.0.0"
echo -n "+$(git log -n1 --format=%h)"
if [[ -n "$(git status -s)" || -n "$(git diff)" || -n "$(git diff --cached)" ]]; then
    echo -n "-$(TZ=UTC date +%Y%m%d%H%M%S)"
fi
