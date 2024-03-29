# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

#!/bin/bash

set -e
set -o pipefail

lightningvcfdir="$1"

chrs=`seq 22`
chrs+=("X" "Y" "M")

for chr in ${chrs[@]}; do
  vcf=`ls $lightningvcfdir/*.chr$chr.*`
  egrep -v ^# $vcf
done
