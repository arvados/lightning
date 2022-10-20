// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	_ "embed"
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os/exec"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
)

type pythonPlot struct{}

//go:embed plot.py
var plotscript string

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
	outputFilename := flags.String("o", "", "output `filename` (e.g., './plot.png')")
	sampleListFilename := flags.String("samples", "", "use second column of `samples.csv` as complete list of sample IDs")
	phenotypeFilename := flags.String("phenotype", "", "use `phenotype.csv` as id->phenotype mapping (column 0 is sample id)")
	cat1Column := flags.Int("phenotype-cat1-column", 1, "0-based column `index` of 1st category in phenotype.csv file")
	cat2Column := flags.Int("phenotype-cat2-column", -1, "0-based column `index` of 2nd category in phenotype.csv file")
	xComponent := flags.Int("x", 1, "1-based PCA component to plot on x axis")
	yComponent := flags.Int("y", 2, "1-based PCA component to plot on y axis")
	priority := flags.Int("priority", 500, "container request priority")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
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
	if !*runlocal {
		err = runner.TranslatePaths(inputFilename, sampleListFilename, phenotypeFilename)
		if err != nil {
			return 1
		}
		*outputFilename = "/mnt/output/plot.png"
	}
	args = []string{
		*inputFilename,
		fmt.Sprintf("%d", *xComponent),
		fmt.Sprintf("%d", *yComponent),
		*sampleListFilename,
		*phenotypeFilename,
		fmt.Sprintf("%d", *cat1Column),
		fmt.Sprintf("%d", *cat2Column),
		*outputFilename,
	}
	if *runlocal {
		if *outputFilename == "" {
			fmt.Fprintln(stderr, "error: must specify -o filename.png in local mode (or try -help)")
			return 1
		}
		cmd := exec.Command("python3", append([]string{"-"}, args...)...)
		cmd.Stdin = strings.NewReader(plotscript)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		err = cmd.Run()
		if err != nil {
			return 1
		}
		return 0
	}
	runner.Prog = "python3"
	runner.Args = append([]string{"/plot.py"}, args...)
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/plot.png")
	return 0
}
