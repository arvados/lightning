# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

$namespaces:
  arv: "http://arvados.org/cwl#"
  cwltool: "http://commonwl.org/cwltool#"
requirements:
  InlineJavascriptRequirement: {}
hints:
  cwltool:LoadListingRequirement:
    loadListing: deep_listing
cwlVersion: v1.0
class: ExpressionTool
label: Scatter over directory to pair VCF, BED and index files
inputs:
  vcfsdir:
    type: Directory
    label: Directory containing compressed VCF, BED, and index files for processing
  bedfile:
    type: File?
    label: Optional BED to scatter over if not included in vcfsdir
outputs:
  vcfs:
    type: File[]
    label: Array of compressed VCF files from input directory
    secondaryFiles: [.tbi]
  beds:
    type: File[]
    label: Array of BED files from input directory
  outnames:
    type: string[]
    label: Array of file names to maintain naming convention for gVCF conversion
expression: |
  ${
    var vcfs = [];
    var beds = [];
    var outnames = [];

    for (var i = 0; i < inputs.vcfsdir.listing.length; i++) {
      var file = inputs.vcfsdir.listing[i];
      if (file.nameext == '.gz') {
        var main = file;
        var baseName = file.nameroot.split(".")[0];
        var mainName = baseName+'.vcf.gz';
        for (var j = 0; j < inputs.vcfsdir.listing.length; j++) {
          var file = inputs.vcfsdir.listing[j];
          if (file.basename == baseName+".tbi") {
            main.secondaryFiles = [file];
          } else if (inputs.bedfile) {
            var bed = inputs.bedfile;
          } else if (file.basename == baseName+".bed") {
            var bed = file;
          }
        }
        vcfs.push(main);
        beds.push(bed);
        outnames.push(mainName);
      }
    }
    return {"vcfs": vcfs, "beds": beds, "outnames": outnames};
  }
