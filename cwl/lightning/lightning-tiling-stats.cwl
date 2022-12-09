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
    arv:dockerCollectionPDH: 1f430e6dd9b6be0ae78d4cffde9b1fef+892
  ResourceRequirement:
    coresMin: 2
    ramMin: 8000
  arv:RuntimeConstraints:
    keep_cache: 10000
    outputDirType: keep_output_dir
inputs:
  libdir: Directory
outputs:
  bed:
    type: File
    outputBinding:
      glob: "*bed"
baseCommand: [lightning, tiling-stats]
arguments:
  - "-local=true"
  - prefix: "-input-dir"
    valueFrom: $(inputs.libdir)
  - prefix: "-output-dir"
    valueFrom: $(runtime.outdir)
