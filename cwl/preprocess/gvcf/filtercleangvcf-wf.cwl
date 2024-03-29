# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

$namespaces:
  arv: "http://arvados.org/cwl#"
  cwltool: "http://commonwl.org/cwltool#"
cwlVersion: v1.0
class: Workflow
label: Filters gVCFs by a specified quality cutoff and cleans
requirements:
  ScatterFeatureRequirement: {}
hints:
  arv:RuntimeConstraints:
    keep_cache: 4096

inputs:
  gvcfdir:
    type: Directory
    label: Input gVCF directory
  cutoff:
    type: int
    label: Filtering cutoff threshold
  keepgqdot:
    type: boolean?
    label: Flag for keeping GQ represented by "."

outputs:
  filteredcleangvcfs:
    type: File[]
    label: Filtered clean gVCFs
    outputSource: filtercleangvcf/filteredcleangvcf

steps:
  getfiles:
    run: getfiles.cwl
    in:
      gvcfdir: gvcfdir
    out: [gvcfs]

  filtercleangvcf:
    run: filtercleangvcf.cwl
    scatter: gvcf
    in:
      gvcf: getfiles/gvcfs
      keepgqdot: keepgqdot
      cutoff: cutoff
    out: [filteredcleangvcf]
