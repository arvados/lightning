// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"

	"git.arvados.org/arvados.git/sdk/go/arvados"
)

type pythonPlot struct{}

func (cmd *pythonPlot) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	inputFilename := flags.String("i", "-", "input `file`")
	sampleListFilename := flags.String("samples", "", "use second column of `samples.csv` as complete list of sample IDs")
	colormapFilename := flags.String("colormap", "", "use first two columns of `colormap.csv` as id->color mapping")
	priority := flags.Int("priority", 500, "container request priority")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}

	runner := arvadosContainerRunner{
		Name:        "lightning plot",
		Client:      arvados.NewClientFromEnv(),
		ProjectUUID: *projectUUID,
		RAM:         4 << 30,
		VCPUs:       1,
		Priority:    *priority,
		Mounts: map[string]map[string]interface{}{
			"/plot.py": map[string]interface{}{
				"kind":    "text",
				"content": plotscript,
			},
		},
	}
	err = runner.TranslatePaths(inputFilename, sampleListFilename, colormapFilename)
	if err != nil {
		return 1
	}
	runner.Prog = "python3"
	runner.Args = []string{"/plot.py", *inputFilename, *sampleListFilename, *colormapFilename, "/mnt/output/plot.png"}
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/plot.png")
	return 0
}

var plotscript = `
import csv
import os
import os.path
import scipy
import sys

infile = sys.argv[1]
X = scipy.load(infile)

colors = None
if sys.argv[2]:
    samples = []
    labels = {}
    with open(sys.argv[2], 'rt') as samplelist:
        for row in csv.reader(samplelist):
            id = row[1]
            samples.append(id)
    with open(sys.argv[3], 'rt') as colormap:
        for row in csv.reader(colormap):
            tag = row[0]
            label = row[1]
            for id in samples:
                if tag in id:
                    labels[id] = label
    colors = []
    labelcolors = {
        'PUR': 'firebrick',
        'CLM': 'firebrick',
        'MXL': 'firebrick',
        'PEL': 'firebrick',
        'TSI': 'green',
        'IBS': 'green',
        'CEU': 'green',
        'GBR': 'green',
        'FIN': 'green',
        'LWK': 'coral',
        'MSL': 'coral',
        'GWD': 'coral',
        'YRI': 'coral',
        'ESN': 'coral',
        'ACB': 'coral',
        'ASW': 'coral',
        'KHV': 'royalblue',
        'CDX': 'royalblue',
        'CHS': 'royalblue',
        'CHB': 'royalblue',
        'JPT': 'royalblue',
        'STU': 'blueviolet',
        'ITU': 'blueviolet',
        'BEB': 'blueviolet',
        'GIH': 'blueviolet',
        'PJL': 'blueviolet',
    }
    for id in samples:
        if (id in labels) and (labels[id] in labelcolors):
            colors.append(labelcolors[labels[id]])
        else:
            colors.append('black')

from matplotlib.figure import Figure
from matplotlib.patches import Polygon
from matplotlib.backends.backend_agg import FigureCanvasAgg
fig = Figure()
ax = fig.add_subplot(111)
ax.scatter(X[:,0], X[:,1], c=colors, s=60, marker='o', alpha=0.5)
canvas = FigureCanvasAgg(fig)
canvas.print_figure(sys.argv[4], dpi=80)
`
