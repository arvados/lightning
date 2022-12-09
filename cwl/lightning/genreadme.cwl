# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

cwlVersion: v1.2
class: CommandLineTool
hints:
  DockerRequirement:
    dockerPull: vcfutil
  ResourceRequirement:
    ramMin: 2000
inputs:
  samplescsv: File
  readmeinfo: string[]
  pythonscript:
    type: File
    default:
      class: File
      location: src/genreadme.py
outputs:
  readme:
    type: stdout
arguments:
  - $(inputs.pythonscript)
  - $(inputs.samplescsv)
  - $(inputs.readmeinfo)
stdout: README
