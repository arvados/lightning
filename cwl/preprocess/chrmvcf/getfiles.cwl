# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.0
class: ExpressionTool
label: Create list of VCFs to process
requirements:
  InlineJavascriptRequirement: {}
inputs:
  dir:
    type: Directory
    label: Input directory of VCFs
outputs:
  vcfs:
    type: File[]
    label: Output VCFs
expression: |
  ${
    var vcfs = [];
    for (var i = 0; i < inputs.dir.listing.length; i++) {
      var file = inputs.dir.listing[i];
      if (file.nameext == ".gz") {
        vcfs.push(file);
      }
    }
    return {"vcfs": vcfs};
  }
