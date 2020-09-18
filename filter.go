package main

import (
	"bufio"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type filterer struct {
	output io.Writer
}

func (cmd *filterer) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	inputFilename := flags.String("i", "-", "input `file`")
	outputFilename := flags.String("o", "-", "output `file`")
	maxvariants := flags.Int("max-variants", -1, "drop tiles with more than `N` variants")
	mincoverage := flags.Float64("min-coverage", 0, "drop tiles with coverage less than `P` across all haplotypes (0 < P ≤ 1)")
	maxtag := flags.Int("max-tag", -1, "drop tiles with tag ID > `N`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	cmd.output = stdout

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if !*runlocal {
		if *outputFilename != "-" {
			err = errors.New("cannot specify output file in container mode: not implemented")
			return 1
		}
		runner := arvadosContainerRunner{
			Name:        "lightning filter",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         64000000000,
			VCPUs:       2,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"filter", "-local=true",
			"-i", *inputFilename,
			"-o", "/mnt/output/library.gob",
			"-max-variants", fmt.Sprintf("%d", *maxvariants),
			"-min-coverage", fmt.Sprintf("%f", *mincoverage),
			"-max-tag", fmt.Sprintf("%d", *maxtag),
		}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/library.gob")
		return 0
	}

	var infile io.ReadCloser
	if *inputFilename == "-" {
		infile = ioutil.NopCloser(stdin)
	} else {
		infile, err = os.Open(*inputFilename)
		if err != nil {
			return 1
		}
		defer infile.Close()
	}
	log.Print("reading")
	cgs, err := ReadCompactGenomes(infile)
	if err != nil {
		return 1
	}
	err = infile.Close()
	if err != nil {
		return 1
	}
	log.Printf("reading done, %d genomes", len(cgs))

	log.Print("filtering")
	ntags := 0
	for _, cg := range cgs {
		if ntags < len(cg.Variants)/2 {
			ntags = len(cg.Variants) / 2
		}
		if *maxvariants < 0 {
			continue
		}
		maxVariantID := tileVariantID(*maxvariants)
		for idx, variant := range cg.Variants {
			if variant > maxVariantID {
				for _, cg := range cgs {
					if len(cg.Variants) > idx {
						cg.Variants[idx & ^1] = 0
						cg.Variants[idx|1] = 0
					}
				}
			}
		}
	}

	if *maxtag >= 0 && ntags > *maxtag {
		ntags = *maxtag
		for i, cg := range cgs {
			if len(cg.Variants) > *maxtag*2 {
				cgs[i].Variants = cg.Variants[:*maxtag*2]
			}
		}
	}

	if *mincoverage > 0 {
		mincov := int(*mincoverage * float64(len(cgs)*2))
		cov := make([]int, ntags)
		for _, cg := range cgs {
			for idx, variant := range cg.Variants {
				if variant > 0 {
					cov[idx>>1]++
				}
			}
		}
		for tag, c := range cov {
			if c < mincov {
				for _, cg := range cgs {
					if len(cg.Variants) > tag*2 {
						cg.Variants[tag*2] = 0
						cg.Variants[tag*2+1] = 0
					}
				}
			}
		}
	}

	log.Print("filtering done")

	var outfile io.WriteCloser
	if *outputFilename == "-" {
		outfile = nopCloser{cmd.output}
	} else {
		outfile, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer outfile.Close()
	}
	w := bufio.NewWriter(outfile)
	enc := gob.NewEncoder(w)
	log.Print("writing")
	err = enc.Encode(LibraryEntry{
		CompactGenomes: cgs,
	})
	if err != nil {
		return 1
	}
	log.Print("writing done")
	err = w.Flush()
	if err != nil {
		return 1
	}
	err = outfile.Close()
	if err != nil {
		return 1
	}
	return 0
}
