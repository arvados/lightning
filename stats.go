package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
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

type stats struct{}

func (cmd *stats) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}

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
			Name:        "lightning stats",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         16000000000,
			VCPUs:       1,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"stats", "-local=true", "-i", *inputFilename, "-o", "/mnt/output/stats.json"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/stats.json")
		return 0
	}

	var input io.ReadCloser
	if *inputFilename == "-" {
		input = ioutil.NopCloser(stdin)
	} else {
		input, err = os.Open(*inputFilename)
		if err != nil {
			return 1
		}
		defer input.Close()
	}

	var output io.WriteCloser
	if *outputFilename == "-" {
		output = nopCloser{stdout}
	} else {
		output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer output.Close()
	}

	bufw := bufio.NewWriter(output)
	cmd.doStats(input, bufw)
	err = bufw.Flush()
	if err != nil {
		return 1
	}
	err = output.Close()
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *stats) doStats(input io.Reader, output io.Writer) error {
	var ret struct {
		Genomes          int
		Tags             int
		TagsPlacedNTimes []int // a[x]==y means there were y tags that placed x times
		TileVariants     int
		VariantsBySize   []int
		NCVariantsBySize []int
	}

	var tagPlacements []int
	dec := gob.NewDecoder(bufio.NewReaderSize(input, 1<<26))
	for {
		var ent LibraryEntry
		err := dec.Decode(&ent)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		ret.Genomes += len(ent.CompactGenomes)
		ret.TileVariants += len(ent.TileVariants)
		if len(ent.TagSet) > 0 {
			if ret.Tags > 0 {
				return errors.New("invalid input: contains multiple tagsets")
			}
			ret.Tags = len(ent.TagSet)
		}
		for _, g := range ent.CompactGenomes {
			if need := (len(g.Variants)+1)/2 - len(tagPlacements); need > 0 {
				tagPlacements = append(tagPlacements, make([]int, need)...)
			}
			for idx, v := range g.Variants {
				if v > 0 {
					tagPlacements[idx/2]++
				}
			}
		}
		for _, tv := range ent.TileVariants {
			if need := 1 + len(tv.Sequence) - len(ret.VariantsBySize); need > 0 {
				ret.VariantsBySize = append(ret.VariantsBySize, make([]int, need)...)
				ret.NCVariantsBySize = append(ret.NCVariantsBySize, make([]int, need)...)
			}

			hasNoCalls := false
			for _, b := range tv.Sequence {
				if b != 'a' && b != 'c' && b != 'g' && b != 't' {
					hasNoCalls = true
				}
			}

			if hasNoCalls {
				ret.NCVariantsBySize[len(tv.Sequence)]++
			} else {
				ret.VariantsBySize[len(tv.Sequence)]++
			}
		}
	}
	for _, p := range tagPlacements {
		for len(ret.TagsPlacedNTimes) <= p {
			ret.TagsPlacedNTimes = append(ret.TagsPlacedNTimes, 0)
		}
		ret.TagsPlacedNTimes[p]++
	}

	return json.NewEncoder(output).Encode(ret)
}