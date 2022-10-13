# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

import csv
import numpy
import os
import os.path
import scipy
import sys

infile = sys.argv[1]
X = numpy.load(infile)

colors = None
category = {}
samples = []
if sys.argv[2]:
    labels = {}
    with open(sys.argv[2], 'rt', newline='') as samplelist:
        for row in csv.reader(samplelist):
            sampleid = row[1]
            samples.append(sampleid)
    phenotype_category_column = int(sys.argv[4])
    phenotype_column = int(sys.argv[5])
    if os.path.isdir(sys.argv[3]):
        phenotype_files = os.scandir(sys.argv[3])
    else:
        phenotype_files = [sys.argv[3]]
    for phenotype_file in phenotype_files:
        with open(phenotype_file, 'rt', newline='') as phenotype:
            dialect = csv.Sniffer().sniff(phenotype.read(1024))
            phenotype.seek(0)
            for row in csv.reader(phenotype, dialect):
                tag = row[0]
                label = row[phenotype_column]
                for sampleid in samples:
                    if tag in sampleid:
                        labels[sampleid] = label
                        if phenotype_category_column >= 0 and row[phenotype_category_column] != '0':
                            category[sampleid] = True
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
        '2': 'green',
        'LWK': 'coral',
        'MSL': 'coral',
        'GWD': 'coral',
        'YRI': 'coral',
        'ESN': 'coral',
        'ACB': 'coral',
        'ASW': 'coral',
        '3': 'coral',
        'KHV': 'royalblue',
        'CDX': 'royalblue',
        'CHS': 'royalblue',
        'CHB': 'royalblue',
        'JPT': 'royalblue',
        '4': 'royalblue',
        'STU': 'blueviolet',
        'ITU': 'blueviolet',
        'BEB': 'blueviolet',
        'GIH': 'blueviolet',
        'PJL': 'blueviolet',
        '5': 'blueviolet',
        '6': 'black',           # unknown?
    }
    for sampleid in samples:
        if (sampleid in labels) and (labels[sampleid] in labelcolors):
            colors.append(labelcolors[labels[sampleid]])
        else:
            colors.append('black')

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
        for i, sampleid in enumerate(samples):
            if category.get(sampleid, False) == (marker == 'x'):
                x.append(X[i,0])
                y.append(X[i,1])
                c.append(colors[i])
    elif marker == 'x':
        continue
    else:
        x = X[:,0]
        y = X[:,1]
        c = None
    ax.scatter(x, y, c=c, s=60, marker=marker, alpha=0.5)
canvas = FigureCanvasAgg(fig)
canvas.print_figure(sys.argv[6], dpi=80)
