# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.2
class: ExpressionTool
requirements:
  InlineJavascriptRequirement: {}
inputs:
  matchgenome: string
  genomeversion: string
outputs:
  libname: string
expression: |
  ${
    var libname = inputs.genomeversion+inputs.matchgenome+"_library";
    return {"libname": libname};
  }
