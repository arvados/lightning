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
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type filter struct {
	MaxVariants int
	MinCoverage float64
	MaxTag      int
}

func (f *filter) Flags(flags *flag.FlagSet) {
	flags.IntVar(&f.MaxVariants, "max-variants", -1, "drop tiles with more than `N` variants")
	flags.Float64Var(&f.MinCoverage, "min-coverage", 0, "drop tiles with coverage less than `P` across all haplotypes (0 < P ≤ 1)")
	flags.IntVar(&f.MaxTag, "max-tag", -1, "drop tiles with tag ID > `N`")
}

func (f *filter) Apply(tilelib *tileLibrary) {
	// Zero out variants at tile positions that have more than
	// f.MaxVariants tile variants.
	if f.MaxVariants >= 0 {
		for tag, variants := range tilelib.variant {
			if f.MaxTag >= 0 && tag >= f.MaxTag {
				break
			}
			if len(variants) <= f.MaxVariants {
				continue
			}
			for _, cg := range tilelib.compactGenomes {
				if len(cg) > tag*2 {
					cg[tag*2] = 0
					cg[tag*2+1] = 0
				}
			}
		}
	}

	// Zero out variants at tile positions that have less than
	// f.MinCoverage.
	mincov := int(2*f.MinCoverage*float64(len(tilelib.compactGenomes)) + 1)
TAG:
	for tag := 0; tag < len(tilelib.variant) && tag < f.MaxTag; tag++ {
		tagcov := 0
		for _, cg := range tilelib.compactGenomes {
			if cg[tag*2] > 0 {
				tagcov++
			}
			if cg[tag*2+1] > 0 {
				tagcov++
			}
			if tagcov >= mincov {
				continue TAG
			}
		}
		for _, cg := range tilelib.compactGenomes {
			cg[tag*2] = 0
			cg[tag*2+1] = 0
		}
	}

	// Truncate genomes and tile data to f.MaxTag (TODO: truncate
	// refseqs too)
	if f.MaxTag >= 0 {
		if len(tilelib.variant) > f.MaxTag {
			tilelib.variant = tilelib.variant[:f.MaxTag]
		}
		for name, cg := range tilelib.compactGenomes {
			if len(cg) > 2*f.MaxTag {
				tilelib.compactGenomes[name] = cg[:2*f.MaxTag]
			}
		}
	}
}

type filtercmd struct {
	output io.Writer
	filter
}

func (cmd *filtercmd) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	cmd.filter.Flags(flags)
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
			"-max-variants", fmt.Sprintf("%d", cmd.MaxVariants),
			"-min-coverage", fmt.Sprintf("%f", cmd.MinCoverage),
			"-max-tag", fmt.Sprintf("%d", cmd.MaxTag),
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
	cgs, err := ReadCompactGenomes(infile, strings.HasSuffix(*inputFilename, ".gz"))
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
		if cmd.MaxVariants < 0 {
			continue
		}
		for idx, variant := range cg.Variants {
			if variant > tileVariantID(cmd.MaxVariants) {
				for _, cg := range cgs {
					if len(cg.Variants) > idx {
						cg.Variants[idx & ^1] = 0
						cg.Variants[idx|1] = 0
					}
				}
			}
		}
	}

	if cmd.MaxTag >= 0 && ntags > cmd.MaxTag {
		ntags = cmd.MaxTag
		for i, cg := range cgs {
			if len(cg.Variants) > cmd.MaxTag*2 {
				cgs[i].Variants = cg.Variants[:cmd.MaxTag*2]
			}
		}
	}

	if cmd.MinCoverage > 0 {
		mincov := int(cmd.MinCoverage * float64(len(cgs)*2))
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
