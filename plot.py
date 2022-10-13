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
if sys.argv[2]:
    samples = []
    labels = {}
    with open(sys.argv[2], 'rt', newline='') as samplelist:
        for row in csv.reader(samplelist):
            sampleid = row[1]
            samples.append(sampleid)
    phenotype_column = int(sys.argv[4])
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
        '6': 'navy',
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
ax.scatter(X[:,0], X[:,1], c=colors, s=60, marker='o', alpha=0.5)
canvas = FigureCanvasAgg(fig)
canvas.print_figure(sys.argv[5], dpi=80)
