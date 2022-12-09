# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.1
class: CommandLineTool
hints:
  DockerRequirement:
    dockerPull: vcfutil
  ResourceRequirement:
    ramMin: 5000
inputs:
  sample: string
  vcf: File
  bashscript:
    type: File
    default:
      class: File
      location: src/getcount.sh
outputs:
  count:
    type: stdout
arguments:
  - $(inputs.bashscript)
  - $(inputs.sample)
  - $(inputs.vcf)
stdout: $(inputs.sample).txt
