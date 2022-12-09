# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.2
class: ExpressionTool
requirements:
  InlineJavascriptRequirement: {}
hints:
  LoadListingRequirement:
    loadListing: shallow_listing
inputs:
  libname: string
  npyfiles: File[]
  onehotnpyfiles: File[]
  pcapngs: File[]
  bed: File
  annotatedvcf: File
  summary: File
outputs:
  stagednpydir: Directory
  stagedonehotnpydir: Directory
  stagedannotationdir: Directory
expression: |
  ${
    var stagednpydir = {"class": "Directory",
                        "basename": "library_full",
                        "listing": inputs.npyfiles};
    var stagedonehotnpydir = {"class": "Directory",
                              "basename": "library_filtered",
                              "listing": inputs.onehotnpyfiles};
    var annotationlist = inputs.pcapngs;
    annotationlist.push(inputs.bed);
    annotationlist.push(inputs.annotatedvcf);
    annotationlist.push(inputs.summary);
    var stagedannotationdir = {"class": "Directory",
                                "basename": inputs.libname+"_annotation",
                                "listing": annotationlist};
    return {"stagednpydir": stagednpydir, "stagedonehotnpydir": stagedonehotnpydir, "stagedannotationdir": stagedannotationdir};
  }
