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

type numpyComVar struct{}

func (cmd *numpyComVar) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	inputFilename := flags.String("i", "-", "numpy matrix `file`")
	priority := flags.Int("priority", 500, "container request priority")
	annotationsFilename := flags.String("annotations", "", "annotations tsv `file`")
	maxResults := flags.Int("max-results", 256, "maximum number of tile variants to output")
	minFrequency := flags.Float64("min-frequency", 0.4, "minimum allele frequency")
	maxFrequency := flags.Float64("max-frequency", 0.6, "maximum allele frequency")
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
		Name:        "lightning numpy-comvar",
		Client:      arvados.NewClientFromEnv(),
		ProjectUUID: *projectUUID,
		RAM:         120000000000,
		VCPUs:       2,
		Priority:    *priority,
	}
	err = runner.TranslatePaths(inputFilename, annotationsFilename)
	if err != nil {
		return 1
	}
	runner.Prog = "python3"
	runner.Args = []string{"-c", `import sys
import scipy
import sys
import csv

numpyFile = sys.argv[1]
annotationsFile = sys.argv[2]
outputFile = sys.argv[3]
maxResults = int(sys.argv[4])
minFrequency = float(sys.argv[5])
maxFrequency = float(sys.argv[6])

out = open(outputFile, 'w')

m = scipy.load(numpyFile)

commonvariants = {}
mincount = m.shape[0] * 2 * minFrequency
maxcount = m.shape[0] * 2 * maxFrequency
for tag in range(m.shape[1] // 2):
  example = {}
  counter = [0, 0, 0, 0, 0]
  for genome in range(m.shape[0]):
    for phase in range(2):
      variant = m[genome][tag*2+phase]
      if variant > 0 and variant < len(counter):
        counter[variant] += 1
        example[variant] = genome
  for variant, count in enumerate(counter):
    if count >= mincount and count <= maxcount:
      commonvariants[tag,variant] = example[variant]
      # sys.stderr.write('tag {} variant {} count {} example {} have {} commonvariants\n'.format(tag, variant, count, example[variant], len(commonvariants)))
  if len(commonvariants) >= maxResults:
    break

found = {}
with open(annotationsFile, newline='') as tsvfile:
  rdr = csv.reader(tsvfile, delimiter='\t', quotechar='"')
  for row in rdr:
    tag = int(row[0])
    variant = int(row[1])
    if (tag, variant) in commonvariants:
      found[tag, variant] = True
      out.write(','.join(row + [str(commonvariants[tag, variant])]) + '\n')
    elif len(found) >= len(commonvariants):
      sys.stderr.write('done\n')
      break

out.close()
`, *inputFilename, *annotationsFilename, "/mnt/output/commonvariants.csv", fmt.Sprintf("%d", *maxResults), fmt.Sprintf("%f", *minFrequency), fmt.Sprintf("%f", *maxFrequency)}
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/commonvariants.csv")
	return 0
}
