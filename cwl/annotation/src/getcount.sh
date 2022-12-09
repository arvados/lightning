#!/bin/bash
# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

set -e
set -o pipefail

sample=$1
vcf=$2

total=`zcat $vcf | awk '!(/^#/)' | wc -l`
rsid=`zcat $vcf | awk '(!(/^#/) && /rs/)' | wc -l`
gnomad=`zcat $vcf | awk '(!(/^#/) && /AF/)' | wc -l`
rsidpercentage=`awk -v n="$rsid" -v d="$total" 'BEGIN {print n/d*100}'`
gnomadpercentage=`awk -v n="$gnomad" -v d="$total" 'BEGIN {print n/d*100}'`

echo "$sample: $total total variants, $rsid variants ($rsidpercentage%) have rsID, $gnomad variants ($gnomadpercentage%) have gnomad AF"
