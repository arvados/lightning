# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

import csv
import numpy
import os
import os.path
import scipy
import sys

(_,
 input_path,
 x_component,
 y_component,
 samples_file,
 phenotype_path,
 phenotype_cat1_column,
 phenotype_cat2_column,
 output_path,
 ) = sys.argv
X = numpy.load(input_path)

colors = None
category = {}
samples = []
if samples_file:
    labels = {}
    with open(samples_file, 'rt', newline='') as samplelist:
        for row in csv.reader(samplelist):
            if row[0] == "Index":
                continue
            sampleid = row[1]
            samples.append(sampleid)
    phenotype_cat2_column = int(phenotype_cat2_column)
    phenotype_cat1_column = int(phenotype_cat1_column)
    if os.path.isdir(phenotype_path):
        phenotype_files = os.scandir(phenotype_path)
    else:
        phenotype_files = [phenotype_path]
    for phenotype_file in phenotype_files:
        with open(phenotype_file, 'rt', newline='') as phenotype:
            dialect = csv.Sniffer().sniff(phenotype.read(1024))
            phenotype.seek(0)
            for row in csv.reader(phenotype, dialect):
                tag = row[0]
                label = row[phenotype_cat1_column]
                for sampleid in samples:
                    if tag in sampleid:
                        labels[sampleid] = label
                        if phenotype_cat2_column >= 0 and row[phenotype_cat2_column] != '0':
                            category[sampleid] = True
    unknown_color = 'grey'
    colors = []
    labelcolors = {
        'PUR': 'firebrick',
        'CLM': 'firebrick',
        'MXL': 'firebrick',
        'PEL': 'firebrick',
        '1': 'firebrick',
        'TSI': 'green',
        'IBS': 'green',
        'CEU': 'green',
        'GBR': 'green',
        'FIN': 'green',
        '5': 'green',
        'LWK': 'coral',
        'MSL': 'coral',
        'GWD': 'coral',
        'YRI': 'coral',
        'ESN': 'coral',
        'ACB': 'coral',
        'ASW': 'coral',
        '4': 'coral',
        'KHV': 'royalblue',
        'CDX': 'royalblue',
        'CHS': 'royalblue',
        'CHB': 'royalblue',
        'JPT': 'royalblue',
        '2': 'royalblue',
        'STU': 'blueviolet',
        'ITU': 'blueviolet',
        'BEB': 'blueviolet',
        'GIH': 'blueviolet',
        'PJL': 'blueviolet',
        '3': 'navy',
    }
    for sampleid in samples:
        if (sampleid in labels) and (labels[sampleid] in labelcolors):
            colors.append(labelcolors[labels[sampleid]])
        else:
            colors.append(unknown_color)

from matplotlib.figure import Figure
from matplotlib.patches import Polygon
from matplotlib.backends.backend_agg import FigureCanvasAgg
fig = Figure()
ax = fig.add_subplot(111)
for marker in ['o', 'x']:
    x = []
    y = []
    if samples:
        c = []
        for unknownfirst in [True, False]:
            for i, sampleid in enumerate(samples):
                if ((colors[i] == unknown_color) == unknownfirst and
                    category.get(sampleid, False) == (marker == 'x')):
                    x.append(X[i,int(x_component)-1])
                    y.append(X[i,int(y_component)-1])
                    c.append(colors[i])
    elif marker == 'x':
        continue
    else:
        x = X[:,int(x_component)-1]
        y = X[:,int(y_component)-1]
        c = None
    ax.scatter(x, y, c=c, s=60, marker=marker, alpha=0.5)
canvas = FigureCanvasAgg(fig)
canvas.print_figure(output_path, dpi=80)
