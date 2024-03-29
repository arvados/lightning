# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.1
class: CommandLineTool
requirements:
  ShellCommandRequirement: {}
hints:
  DockerRequirement:
    dockerPull: vcfutil
  ResourceRequirement:
    ramMin: 5000
inputs:
  sample: string
  vcf: File
outputs:
  imputedvcf:
    type: File
    outputBinding:
      glob: "*.vcf.gz"
    secondaryFiles: [.tbi]
baseCommand: zcat
arguments:
  - $(inputs.vcf)
  - shellQuote: false
    valueFrom: "|"
  - "egrep"
  - "^#|IMP"
  - shellQuote: false
    valueFrom: "|"
  - "egrep"
  - prefix: "-v"
    valueFrom: '0\|0'
  - shellQuote: false
    valueFrom: "|"
  - "bgzip"
  - "-c"
  - shellQuote: false
    valueFrom: ">"
  - $(inputs.sample).vcf.gz
  - shellQuote: false
    valueFrom: "&&"
  - "tabix"
  - $(inputs.sample).vcf.gz
