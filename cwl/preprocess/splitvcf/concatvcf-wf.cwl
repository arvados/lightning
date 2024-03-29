# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
cwlVersion: v1.0
class: Workflow
label: Concatenate a set of VCFs split by chromosomes
requirements:
  ScatterFeatureRequirement: {}
hints:
  cwltool:LoadListingRequirement:
    loadListing: shallow_listing
inputs:
  vcfdirs:
    type: Directory[]
    label: Input VCFs directories

outputs:
  vcfs:
    type: File[]
    label: Concatenated VCFs
    outputSource: concatvcf/vcf
    secondaryFiles: [.tbi]

steps:
  concatvcf:
    run: concatvcf.cwl
    scatter: vcfdir
    in:
      vcfdir: vcfdirs
    out: [vcf]
