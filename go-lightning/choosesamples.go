// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type chooseSamples struct {
	filter filter
}

func (cmd *chooseSamples) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := cmd.run(prog, args, stdin, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

func (cmd *chooseSamples) run(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	inputDir := flags.String("input-dir", "./in", "input `directory`")
	outputDir := flags.String("output-dir", "./out", "output `directory`")
	trainingSetSize := flags.Float64("training-set-size", 0.8, "number (or proportion, if <=1) of eligible samples to assign to the training set")
	caseControlFilename := flags.String("case-control-file", "", "tsv file or directory indicating cases and controls (if directory, all .tsv files will be read)")
	caseControlColumn := flags.String("case-control-column", "", "name of case/control column in case-control files (value must be 0 for control, 1 for case)")
	randSeed := flags.Int64("random-seed", 0, "PRNG seed")
	cmd.filter.Flags(flags)
	err := flags.Parse(args)
	if err == flag.ErrHelp {
		return nil
	} else if err != nil {
		return err
	} else if flags.NArg() > 0 {
		return fmt.Errorf("errant command line arguments after parsed flags: %v", flags.Args())
	}
	if (*caseControlFilename == "") != (*caseControlColumn == "") {
		return errors.New("must provide both -case-control-file and -case-control-column, or neither")
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if !*runlocal {
		runner := arvadosContainerRunner{
			Name:        "lightning choose-samples",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         16000000000,
			VCPUs:       4,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir, caseControlFilename)
		if err != nil {
			return err
		}
		runner.Args = []string{"choose-samples", "-local=true",
			"-pprof=:6060",
			"-input-dir=" + *inputDir,
			"-output-dir=/mnt/output",
			"-case-control-file=" + *caseControlFilename,
			"-case-control-column=" + *caseControlColumn,
			"-training-set-size=" + fmt.Sprintf("%f", *trainingSetSize),
			"-random-seed=" + fmt.Sprintf("%d", *randSeed),
		}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, output)
		return nil
	}

	infiles, err := allFiles(*inputDir, matchGobFile)
	if err != nil {
		return err
	}
	if len(infiles) == 0 {
		err = fmt.Errorf("no input files found in %s", *inputDir)
		return err
	}
	sort.Strings(infiles)

	in0, err := open(infiles[0])
	if err != nil {
		return err
	}

	matchGenome, err := regexp.Compile(cmd.filter.MatchGenome)
	if err != nil {
		err = fmt.Errorf("-match-genome: invalid regexp: %q", cmd.filter.MatchGenome)
		return err
	}

	var sampleIDs []string
	err = DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
		for _, cg := range ent.CompactGenomes {
			if matchGenome.MatchString(cg.Name) {
				sampleIDs = append(sampleIDs, cg.Name)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	in0.Close()

	if len(sampleIDs) == 0 {
		err = fmt.Errorf("no genomes found matching regexp %q", cmd.filter.MatchGenome)
		return err
	}
	sort.Strings(sampleIDs)
	caseControl, err := cmd.loadCaseControlFiles(*caseControlFilename, *caseControlColumn, sampleIDs)
	if err != nil {
		return err
	}
	if len(caseControl) == 0 {
		err = fmt.Errorf("fatal: 0 cases, 0 controls, nothing to do")
		return err
	}

	var trainingSet, validationSet []int
	for i := range caseControl {
		trainingSet = append(trainingSet, i)
	}
	sort.Ints(trainingSet)
	wantlen := int(*trainingSetSize)
	if *trainingSetSize <= 1 {
		wantlen = int(*trainingSetSize * float64(len(trainingSet)))
	}
	randsrc := rand.NewSource(*randSeed)
	for tslen := len(trainingSet); tslen > wantlen; {
		i := int(randsrc.Int63()) % tslen
		validationSet = append(validationSet, trainingSet[i])
		tslen--
		trainingSet[i] = trainingSet[tslen]
		trainingSet = trainingSet[:tslen]
	}
	sort.Ints(trainingSet)
	sort.Ints(validationSet)

	samplesFilename := *outputDir + "/samples.csv"
	log.Infof("writing sample metadata to %s", samplesFilename)
	var f *os.File
	f, err = os.Create(samplesFilename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprint(f, "Index,SampleID,CaseControl,TrainingValidation\n")
	if err != nil {
		return err
	}
	tsi := 0 // next idx in training set
	vsi := 0 // next idx in validation set
	for i, name := range sampleIDs {
		var cc, tv string
		if len(trainingSet) > tsi && trainingSet[tsi] == i {
			tv = "1"
			tsi++
			if caseControl[i] {
				cc = "1"
			} else {
				cc = "0"
			}
		} else if len(validationSet) > vsi && validationSet[vsi] == i {
			tv = "0"
			vsi++
			if caseControl[i] {
				cc = "1"
			} else {
				cc = "0"
			}
		}
		_, err = fmt.Fprintf(f, "%d,%s,%s,%s\n", i, trimFilenameForLabel(name), cc, tv)
		if err != nil {
			err = fmt.Errorf("write %s: %w", samplesFilename, err)
			return err
		}
	}
	err = f.Close()
	if err != nil {
		err = fmt.Errorf("close %s: %w", samplesFilename, err)
		return err
	}
	return nil
}

// Read case/control file(s). Returned map m has m[i]==true if
// sampleIDs[i] is case, m[i]==false if sampleIDs[i] is control.
func (cmd *chooseSamples) loadCaseControlFiles(path, colname string, sampleIDs []string) (map[int]bool, error) {
	if path == "" {
		// all samples are control group
		cc := make(map[int]bool, len(sampleIDs))
		for i := range sampleIDs {
			cc[i] = false
		}
		return cc, nil
	}
	infiles, err := allFiles(path, nil)
	if err != nil {
		return nil, err
	}
	// index in sampleIDs => case(true) / control(false)
	cc := map[int]bool{}
	// index in sampleIDs => true if matched by multiple patterns in case/control files
	dup := map[int]bool{}
	for _, infile := range infiles {
		f, err := open(infile)
		if err != nil {
			return nil, err
		}
		buf, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return nil, err
		}
		ccCol := -1
		for _, tsv := range bytes.Split(buf, []byte{'\n'}) {
			if len(tsv) == 0 {
				continue
			}
			split := strings.Split(string(tsv), "\t")
			if ccCol < 0 {
				// header row
				for col, name := range split {
					if name == colname {
						ccCol = col
						break
					}
				}
				if ccCol < 0 {
					return nil, fmt.Errorf("%s: no column named %q in header row %q", infile, colname, tsv)
				}
				continue
			}
			if len(split) <= ccCol {
				continue
			}
			pattern := split[0]
			found := -1
			for i, name := range sampleIDs {
				if strings.Contains(name, pattern) {
					if found >= 0 {
						log.Warnf("pattern %q in %s matches multiple sample IDs (%q, %q)", pattern, infile, sampleIDs[found], name)
					}
					if dup[i] {
						continue
					} else if _, ok := cc[i]; ok {
						log.Warnf("multiple patterns match sample ID %q, omitting from cases/controls", name)
						dup[i] = true
						delete(cc, i)
						continue
					}
					found = i
					if split[ccCol] == "0" {
						cc[found] = false
					}
					if split[ccCol] == "1" {
						cc[found] = true
					}
				}
			}
			if found < 0 {
				log.Warnf("pattern %q in %s does not match any genome IDs", pattern, infile)
				continue
			}
		}
	}
	return cc, nil
}
