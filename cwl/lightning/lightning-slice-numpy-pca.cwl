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
    ramMin: 1500000
  arv:RuntimeConstraints:
    keep_cache: 83000
    outputDirType: keep_output_dir
inputs:
  matchgenome: string
  libdir: Directory
  regions: File?
  threads: int
  mergeoutput: string
  expandregions: int
  samplescsv: File
  pcacomponents: int
outputs:
  outdir:
    type: Directory
    outputBinding:
      glob: "."
  pcanpy:
    type: File
    outputBinding:
      glob: "pca.npy"
  pcasamplescsv:
    type: File
    outputBinding:
      glob: "samples.csv"
baseCommand: [lightning, slice-numpy]
arguments:
  - "-local=true"
  - prefix: "-input-dir="
    valueFrom: $(inputs.libdir)
    separate: false
  - prefix: "-output-dir="
    valueFrom: $(runtime.outdir)
    separate: false
  - prefix: "-match-genome="
    valueFrom: $(inputs.matchgenome)
    separate: false
  - prefix: "-regions="
    valueFrom: $(inputs.regions)
    separate: false
  - prefix: "-threads="
    valueFrom: $(inputs.threads)
    separate: false
  - prefix: "-merge-output="
    valueFrom: $(inputs.mergeoutput)
    separate: false
  - prefix: "-expand-regions="
    valueFrom: $(inputs.expandregions)
    separate: false
  - prefix: "-samples="
    valueFrom: $(inputs.samplescsv)
    separate: false
  - "-pca=true"
  - prefix: "-pca-components="
    valueFrom: $(inputs.pcacomponents)
    separate: false
  - "-min-coverage=0.98"
  - "-max-pca-tiles=100000"
