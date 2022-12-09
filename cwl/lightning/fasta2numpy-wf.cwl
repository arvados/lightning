# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

$namespaces:
  arv: "http://arvados.org/cwl#"
cwlVersion: v1.2
class: Workflow
requirements:
  ScatterFeatureRequirement: {}
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  MultipleInputFeatureRequirement: {}

inputs:
  tagset:
    type: File
  fastadirs:
    type:
      type: array
      items: Directory
  refdir:
    type: Directory
  batchsize:
    type: int
  regions:
    type: File?
  matchgenome:
    type: string
  threads:
    type: int
  mergeoutput:
    type: string
  expandregions:
    type: int
  phenotypesnofamilydir:
    type: Directory
  phenotypesdir:
    type: Directory
  trainingsetsize:
    type: float
  randomseed:
    type: int
  pcacomponents:
    type: int
  chrs: string[]
  snpeffdatadir: Directory
  genomeversion: string
  dbsnp:
    type: File
    secondaryFiles: [.csi]
  gnomaddir: Directory
  readmeinfo: string[]

outputs:
  stagednpydir:
    type: Directory
    outputSource: stage-output/stagednpydir
  stagedonehotnpydir:
    type: Directory
    outputSource: stage-output/stagedonehotnpydir
  stagedannotationdir:
    type: Directory
    outputSource: stage-output/stagedannotationdir
  readme:
    type: File
    outputSource: genreadme/readme

steps:
  batch-dirs:
    run: batch-dirs.cwl
    in:
      dirs: fastadirs
      batchsize: batchsize
    out: [batches]

  lightning-import_data:
    run: lightning-import.cwl
    scatter: fastadirs
    in:
      saveincomplete:
        valueFrom: "false"
      tagset: tagset
      fastadirs: batch-dirs/batches
    out: [lib]

  lightning-import_refs:
    run: lightning-import.cwl
    in:
      saveincomplete:
        valueFrom: "true"
      tagset: tagset
      fastadirs: refdir
    out: [lib]

  lightning-slice:
    run: lightning-slice.cwl
    in:
      datalibs: lightning-import_data/lib
      reflib: lightning-import_refs/lib
    out: [libdir]

  lightning-tiling-stats:
    run: lightning-tiling-stats.cwl
    in:
      libdir: lightning-slice/libdir
    out: [bed]

  lightning-choose-samples:
    run: lightning-choose-samples.cwl
    in:
      matchgenome: matchgenome
      libdir: lightning-slice/libdir
      phenotypesdir: phenotypesnofamilydir
      trainingsetsize: trainingsetsize
      randomseed: randomseed
    out: [samplescsv]

  lightning-slice-numpy:
    run: lightning-slice-numpy.cwl
    in:
      matchgenome: matchgenome
      libdir: lightning-slice/libdir
      regions: regions
      threads: threads
      mergeoutput: mergeoutput
      expandregions: expandregions
      samplescsv: lightning-choose-samples/samplescsv
    out: [outdir, npys, chunktagoffsetcsv]

  lightning-slice-numpy-onehot:
    run: lightning-slice-numpy-onehot.cwl
    in:
      matchgenome: matchgenome
      libdir: lightning-slice/libdir
      regions: regions
      threads: threads
      mergeoutput: mergeoutput
      expandregions: expandregions
      samplescsv: lightning-choose-samples/samplescsv
    out: [outdir, npys]

  lightning-slice-numpy-pca:
    run: lightning-slice-numpy-pca.cwl
    in:
      matchgenome: matchgenome
      libdir: lightning-slice/libdir
      regions: regions
      threads: threads
      mergeoutput: mergeoutput
      expandregions: expandregions
      samplescsv: lightning-choose-samples/samplescsv
      pcacomponents: pcacomponents
    out: [outdir, pcanpy, pcasamplescsv]

  lightning-plot_1-2:
    run: lightning-plot.cwl
    in:
      pcanpy: lightning-slice-numpy-pca/pcanpy
      pcasamplescsv: lightning-slice-numpy-pca/pcasamplescsv
      phenotypesdir: phenotypesdir
      xcomponent:
        valueFrom: "1"
      ycomponent:
        valueFrom: "2"
    out: [png]

  lightning-plot_2-3:
    run: lightning-plot.cwl
    in:
      pcanpy: lightning-slice-numpy-pca/pcanpy
      pcasamplescsv: lightning-slice-numpy-pca/pcasamplescsv
      phenotypesdir: phenotypesdir
      xcomponent:
        valueFrom: "2"
      ycomponent:
        valueFrom: "3"
    out: [png]

  lightning-anno2vcf-onehot:
    run: lightning-anno2vcf.cwl
    in:
      annodir: lightning-slice-numpy-onehot/outdir
    out: [vcfdir]

  make-libname:
    run: make-libname.cwl
    in:
      matchgenome: matchgenome
      genomeversion: genomeversion
    out: [libname]

  annotate-wf:
    run: ../annotation/annotate-wf.cwl
    in:
      sample: make-libname/libname
      chrs: chrs
      vcfdir: lightning-anno2vcf-onehot/vcfdir
      snpeffdatadir: snpeffdatadir
      genomeversion: genomeversion
      dbsnp: dbsnp
      gnomaddir: gnomaddir
    out: [annotatedvcf, summary]

  stage-output:
    run: stage-output.cwl
    in:
      libname: make-libname/libname
      npyfiles:
        source: [lightning-slice-numpy/npys, lightning-slice-numpy/chunktagoffsetcsv]
        linkMerge: merge_flattened
      onehotnpyfiles: lightning-slice-numpy-onehot/npys
      pcapngs:
        source: [lightning-plot_1-2/png, lightning-plot_2-3/png]
        linkMerge: merge_flattened
      bed: lightning-tiling-stats/bed
      annotatedvcf: annotate-wf/annotatedvcf
      summary: annotate-wf/summary
    out: [stagednpydir, stagedonehotnpydir, stagedannotationdir]

  genreadme:
    run: genreadme.cwl
    in:
      samplescsv: lightning-choose-samples/samplescsv
      readmeinfo: readmeinfo
    out: [readme]
