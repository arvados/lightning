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

type goPCA struct{}

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
			RAM:         432000000000,
			VCPUs:       2,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"pca-go", "-local=true", fmt.Sprintf("-one-hot=%v", *onehot), "-i", *inputFilename, "-o", "/mnt/output/pca.npy"}
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
	cgs, err := ReadCompactGenomes(input)
	if err != nil {
		return 1
	}
	err = input.Close()
	if err != nil {
		return 1
	}
	sort.Slice(cgs, func(i, j int) bool { return cgs[i].Name < cgs[j].Name })

	data, rows, cols := cgs2array(cgs)
	if *onehot {
		data, cols = recodeOnehot(data, cols)
	}
	pca, err := nlp.NewPCA(*components).FitTransform(array2matrix(rows, cols, data))
	if err != nil {
		return 1
	}

	rows, cols = pca.Dims()
	out := make([]float64, rows*cols)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			out[i*cols+j] = pca.At(i, j)
		}
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
	npw, err := gonpy.NewWriter(nopCloser{bufw})
	if err != nil {
		return 1
	}
	npw.Shape = []int{rows, cols}
	npw.WriteFloat64(out)
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

func array2matrix(rows, cols int, data []uint16) mat.Matrix {
	floatdata := make([]float64, rows*cols)
	for i, v := range data {
		floatdata[i] = float64(v)
	}
	return mat.NewDense(rows, cols, floatdata)
}
