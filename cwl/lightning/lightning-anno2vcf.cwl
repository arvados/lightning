# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

$namespaces:
  arv: "http://arvados.org/cwl#"
cwlVersion: v1.2
class: CommandLineTool
requirements:
  NetworkAccess:
    networkAccess: true
hints:
  DockerRequirement:
    dockerPull: lightning
  ResourceRequirement:
    coresMin: 64
    ramMin: 500000
  arv:RuntimeConstraints:
    keep_cache: 83000
    outputDirType: keep_output_dir
inputs:
  annodir: Directory
outputs:
  vcfdir:
    type: Directory
    outputBinding:
      glob: "."
baseCommand: [lightning, anno2vcf]
arguments:
  - "-local=true"
  - prefix: "-input-dir="
    valueFrom: $(inputs.annodir)
    separate: false
  - prefix: "-output-dir="
    valueFrom: $(runtime.outdir)
    separate: false
