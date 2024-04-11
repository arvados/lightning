#!/bin/bash

# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

set -e -o pipefail
echo -n "0.0.0"
echo -n "+$(git log -n1 --format=%h)"
if [[ -n "$(git status -s)" || -n "$(git diff)" || -n "$(git diff --cached)" ]]; then
    echo -n "-$(TZ=UTC date +%Y%m%d%H%M%S)"
fi
