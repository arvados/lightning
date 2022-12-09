# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

#!/bin/sh

REFERENCE=$1
CGIVAR=$2

cgatools mkvcf --beta --reference $REFERENCE --include-no-calls --field-names GT,GQ,DP,AD --source-names masterVar --master-var $CGIVAR || true
