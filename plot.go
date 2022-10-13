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
	sampleListFilename := flags.String("samples", "", "use second column of `samples.csv` as complete list of sample IDs")
	phenotypeFilename := flags.String("phenotype", "", "use `phenotype.csv` as id->phenotype mapping (column 0 is sample id)")
	phenotypeColumn := flags.Int("phenotype-column", 1, "0-based column `index` of phenotype in phenotype.csv file")
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
	err = runner.TranslatePaths(inputFilename, sampleListFilename, phenotypeFilename)
	if err != nil {
		return 1
	}
	runner.Prog = "python3"
	runner.Args = []string{"/plot.py", *inputFilename, *sampleListFilename, *phenotypeFilename, fmt.Sprintf("%d", *phenotypeColumn), "/mnt/output/plot.png"}
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/plot.png")
	return 0
}
