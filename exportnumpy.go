package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/kshedden/gonpy"
	log "github.com/sirupsen/logrus"
)

type exportNumpy struct{}

func (cmd *exportNumpy) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	onehot := flags.Bool("one-hot", false, "recode tile variants as one-hot")
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
			Name:        "lightning export-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         128000000000,
			VCPUs:       2,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"export-numpy", "-local=true", fmt.Sprintf("-one-hot=%v", *onehot), "-i", *inputFilename, "-o", "/mnt/output/library.npy"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/library.npy")
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
	cgs, err := ReadCompactGenomes(input)
	if err != nil {
		return 1
	}
	err = input.Close()
	if err != nil {
		return 1
	}
	sort.Slice(cgs, func(i, j int) bool { return cgs[i].Name < cgs[j].Name })

	out, rows, cols := cgs2array(cgs)

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
	npw, err := gonpy.NewWriter(nopCloser{bufw})
	if err != nil {
		return 1
	}
	if *onehot {
		out, cols = recodeOnehot(out, cols)
	}
	npw.Shape = []int{rows, cols}
	npw.WriteUint16(out)
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

func cgs2array(cgs []CompactGenome) (data []uint16, rows, cols int) {
	rows = len(cgs)
	for _, cg := range cgs {
		if cols < len(cg.Variants) {
			cols = len(cg.Variants)
		}
	}
	data = make([]uint16, rows*cols)
	for row, cg := range cgs {
		for i, v := range cg.Variants {
			data[row*cols+i] = uint16(v)
		}
	}
	return
}

func recodeOnehot(in []uint16, incols int) ([]uint16, int) {
	rows := len(in) / incols
	maxvalue := make([]uint16, incols)
	for row := 0; row < rows; row++ {
		for col := 0; col < incols; col++ {
			if v := in[row*incols+col]; maxvalue[col] < v {
				maxvalue[col] = v
			}
		}
	}
	outcol := make([]int, incols)
	outcols := 0
	for incol, v := range maxvalue {
		outcol[incol] = outcols
		outcols += int(v)
	}
	out := make([]uint16, rows*outcols)
	for row := 0; row < rows; row++ {
		for col := 0; col < incols; col++ {
			if v := in[row*incols+col]; v > 0 {
				out[row*outcols+outcol[col]+int(v)-1] = 1
			}
		}
	}
	return out, outcols
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }
