# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

import csv
import os
import sys

import matplotlib
import numpy
import pandas
import qmplot

(_,
 input_path,
 output_path,
 ) = sys.argv
columns = numpy.load(os.path.join(input_path, 'onehot-columns.npy'))

# pvalue maps tag# => [pvalue1, pvalue2, ...] (one het p-value and one hom p-value for each tile variant)
pvalue = {}
for i in range(columns.shape[1]):
    tag = columns[0,i]
    x = pvalue.get(tag, [])
    x.append(pow(10, -columns[4,i] / 1000000))
    pvalue[tag] = x

# tilepos maps tag# => (chromosome, position)
tilepos = {}
for dirent in os.scandir(input_path):
    if dirent.name.endswith('.annotations.csv'):
        with open(dirent, 'rt', newline='') as annotations:
            for annotation in csv.reader(annotations):
                # 500000,0,2,=,chr1,160793649,,,
                if annotation[3] == "=":
                    tilepos[int(annotation[0])] = (annotation[4], int(annotation[5]))

series = {"#CHROM": [], "POS": [], "P": []}
for tag, chrpos in sorted(tilepos.items(), key=lambda item: (item[1][0][-1] > '9', item[1][0].lstrip('chr').zfill(2), item[1][1])):
    for p in pvalue.get(tag, []):
        series["#CHROM"].append(chrpos[0])
        series["POS"].append(chrpos[1])
        series["P"].append(p)

qmplot.manhattanplot(data=pandas.DataFrame(series),
                     suggestiveline=2e-10,  # Turn off suggestiveline
                     genomewideline=2e-11,  # Turn off genomewidel
                     sign_line_cols=["#D62728", "#2CA02C"],
                     marker=".",
                     alpha = 0.6,
                     hline_kws={"linestyle": "--", "lw": 1.3},
                     title="Tile Variant Manhattan Plot",
                     # xtick_label_set=xtick,
                     xlabel="Chromosome",
                     ylabel=r"$-log_{10}{(P)}$",
                     xticklabel_kws={"rotation": "vertical"})
matplotlib.pyplot.savefig(output_path, bbox_inches="tight")
