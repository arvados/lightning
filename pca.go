// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"context"
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
	"github.com/james-bowman/nlp"
	"github.com/kshedden/gonpy"
	log "github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/mat"
)

type pythonPCA struct{}

func (cmd *pythonPCA) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	priority := flags.Int("priority", 500, "container request priority")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}

	runner := arvadosContainerRunner{
		Name:        "lightning pca",
		Client:      arvados.NewClientFromEnv(),
		ProjectUUID: *projectUUID,
		RAM:         440000000000,
		VCPUs:       1,
		Priority:    *priority,
	}
	err = runner.TranslatePaths(inputFilename)
	if err != nil {
		return 1
	}
	runner.Prog = "python3"
	runner.Args = []string{"-c", `import sys
import scipy
from sklearn.decomposition import PCA
scipy.save(sys.argv[2], PCA(n_components=4).fit_transform(scipy.load(sys.argv[1])))`, *inputFilename, "/mnt/output/pca.npy"}
	var output string
	output, err = runner.Run()
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, output+"/pca.npy")
	return 0
}

type goPCA struct {
	filter filter
}

func (cmd *goPCA) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	components := flags.Int("components", 4, "number of components")
	onehot := flags.Bool("one-hot", false, "recode tile variants as one-hot")
	cmd.filter.Flags(flags)
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
			Name:        "lightning pca-go",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         300000000000, // maybe 10x input size?
			VCPUs:       16,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"pca-go", "-local=true", fmt.Sprintf("-one-hot=%v", *onehot), "-i", *inputFilename, "-o", "/mnt/output/pca.npy"}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/pca.npy")
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
	log.Print("reading")
	tilelib := &tileLibrary{
		retainNoCalls:  true,
		compactGenomes: map[string][]tileVariantID{},
	}
	err = tilelib.LoadGob(context.Background(), input, strings.HasSuffix(*inputFilename, ".gz"))
	if err != nil {
		return 1
	}
	err = input.Close()
	if err != nil {
		return 1
	}

	log.Info("filtering")
	cmd.filter.Apply(tilelib)
	log.Info("tidying")
	tilelib.Tidy()

	log.Print("converting cgs to array")
	data, rows, cols := cgs2array(tilelib, cgnames(tilelib), lowqual(tilelib), nil, 0, len(tilelib.variant))
	if *onehot {
		log.Printf("recode one-hot: %d rows, %d cols", rows, cols)
		data, _, cols = recodeOnehot(data, cols)
	}
	tilelib = nil

	log.Printf("creating matrix backed by array: %d rows, %d cols", rows, cols)
	mtx := array2matrix(rows, cols, data).T()

	log.Print("fitting")
	transformer := nlp.NewPCA(*components)
	transformer.Fit(mtx)
	log.Printf("transforming")
	mtx, err = transformer.Transform(mtx)
	if err != nil {
		return 1
	}
	mtx = mtx.T()

	rows, cols = mtx.Dims()
	log.Printf("copying result to numpy output array: %d rows, %d cols", rows, cols)
	out := make([]float64, rows*cols)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			out[i*cols+j] = mtx.At(i, j)
		}
	}

	var output io.WriteCloser
	if *outputFilename == "-" {
		output = nopCloser{stdout}
	} else {
		output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
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
	npw.Shape = []int{rows, cols}
	log.Printf("writing numpy: %d rows, %d cols", rows, cols)
	npw.WriteFloat64(out)
	err = bufw.Flush()
	if err != nil {
		return 1
	}
	err = output.Close()
	if err != nil {
		return 1
	}
	log.Print("done")
	return 0
}

func array2matrix(rows, cols int, data []int16) mat.Matrix {
	floatdata := make([]float64, rows*cols)
	for i, v := range data {
		floatdata[i] = float64(v)
	}
	return mat.NewDense(rows, cols, floatdata)
}
