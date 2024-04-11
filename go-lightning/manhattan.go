// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	_ "embed"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
)

type manhattanPlot struct{}

//go:embed manhattan.py
var manhattanPy string

func (cmd *manhattanPlot) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	inputDirectory := flags.String("i", "-", "input `directory` (output of slice-numpy -single-onehot)")
	outputFilename := flags.String("o", "", "output `filename` (e.g., './plot.png')")
	csvOutputFilename := flags.String("csv-output", "", "csv output `filename` (e.g., './tile-locations-pvalues.csv')")
	csvOutputThreshold := flags.Float64("csv-output-threshold", 0, "logpvalue threshold for csv output (0 for none)")
	priority := flags.Int("priority", 500, "container request priority")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if flags.NArg() > 0 {
		err = fmt.Errorf("errant command line arguments after parsed flags: %v", flags.Args())
		return 2
	}

	runner := arvadosContainerRunner{
		Name:        "lightning manhattan",
		Client:      arvados.NewClientFromEnv(),
		ProjectUUID: *projectUUID,
		RAM:         4 << 30,
		VCPUs:       1,
		Priority:    *priority,
		Mounts: map[string]map[string]interface{}{
			"/manhattan.py": map[string]interface{}{
				"kind":    "text",
				"content": manhattanPy,
			},
		},
	}
	if !*runlocal {
		err = runner.TranslatePaths(inputDirectory)
		if err != nil {
			return 1
		}
		*outputFilename = "/mnt/output/plot.png"
		*csvOutputFilename = "/mnt/output/tile-locations-pvalues.csv"
	}
	args = []string{
		*inputDirectory,
		*outputFilename,
		fmt.Sprintf("%g", *csvOutputThreshold),
		*csvOutputFilename,
	}
	if *runlocal {
		if *outputFilename == "" {
			fmt.Fprintln(stderr, "error: must specify -o filename.png in local mode (or try -help)")
			return 1
		}
		cmd := exec.Command("python3", append([]string{"-"}, args...)...)
		cmd.Stdin = strings.NewReader(manhattanPy)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		err = cmd.Run()
		if err != nil {
			return 1
		}
		return 0
	}
	runner.Prog = "python3"
	runner.Args = append([]string{"/manhattan.py"}, args...)
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/plot.png")
	return 0
}
