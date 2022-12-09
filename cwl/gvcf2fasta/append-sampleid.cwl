# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.1
class: ExpressionTool
inputs:
  sampleid: string
  suffix: string
outputs:
  appendedsampleid: string
requirements:
  InlineJavascriptRequirement: {}
expression: |
  ${
    var appendedsampleid = inputs.sampleid + inputs.suffix;
    return {"appendedsampleid": appendedsampleid};
  }
