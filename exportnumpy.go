package main

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
	"path"
	"sort"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/kshedden/gonpy"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type exportNumpy struct {
	filter filter
}

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
	annotationsFilename := flags.String("output-annotations", "", "output `file` for tile variant annotations csv")
	librefsFilename := flags.String("output-onehot2tilevar", "", "when using -one-hot, create csv `file` mapping column# to tag# and variant#")
	labelsFilename := flags.String("output-labels", "", "output `file` for genome labels csv")
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
			Name:        "lightning export-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         450000000000,
			VCPUs:       32,
			Priority:    *priority,
			KeepCache:   1,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"export-numpy", "-local=true",
			fmt.Sprintf("-one-hot=%v", *onehot),
			"-i", *inputFilename,
			"-o", "/mnt/output/matrix.npy",
			"-output-annotations", "/mnt/output/annotations.csv",
			"-output-onehot2tilevar", "/mnt/output/onehot2tilevar.csv",
			"-output-labels", "/mnt/output/labels.csv",
			"-max-variants", fmt.Sprintf("%d", cmd.filter.MaxVariants),
			"-min-coverage", fmt.Sprintf("%f", cmd.filter.MinCoverage),
			"-max-tag", fmt.Sprintf("%d", cmd.filter.MaxTag),
		}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/matrix.npy")
		return 0
	}

	var input io.ReadCloser
	if *inputFilename == "-" {
		input = ioutil.NopCloser(stdin)
	} else {
		input, err = open(*inputFilename)
		if err != nil {
			return 1
		}
		defer input.Close()
	}
	input = ioutil.NopCloser(bufio.NewReaderSize(input, 8*1024*1024))
	tilelib := &tileLibrary{
		retainNoCalls:       true,
		retainTileSequences: true,
		compactGenomes:      map[string][]tileVariantID{},
	}
	err = tilelib.LoadGob(context.Background(), input, strings.HasSuffix(*inputFilename, ".gz"), nil)
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

	if *annotationsFilename != "" {
		log.Infof("writing annotations")
		var annow io.WriteCloser
		annow, err = os.OpenFile(*annotationsFilename, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
		defer annow.Close()
		err = (&annotatecmd{maxTileSize: 5000}).exportTileDiffs(annow, tilelib)
		if err != nil {
			return 1
		}
		err = annow.Close()
		if err != nil {
			return 1
		}
	}

	log.Info("building numpy array")
	out, rows, cols, names := cgs2array(tilelib)

	if *labelsFilename != "" {
		log.Infof("writing labels to %s", *labelsFilename)
		var f *os.File
		f, err = os.OpenFile(*labelsFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer f.Close()
		_, outBasename := path.Split(*outputFilename)
		for i, name := range names {
			_, err = fmt.Fprintf(f, "%d,%q,%q\n", i, trimFilenameForLabel(name), outBasename)
			if err != nil {
				err = fmt.Errorf("write %s: %w", *labelsFilename, err)
				return 1
			}
		}
		err = f.Close()
		if err != nil {
			err = fmt.Errorf("close %s: %w", *labelsFilename, err)
			return 1
		}
	}

	log.Info("writing numpy file")
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
		log.Info("recoding to onehot")
		recoded, librefs, recodedcols := recodeOnehot(out, cols)
		out, cols = recoded, recodedcols
		if *librefsFilename != "" {
			log.Infof("writing onehot column mapping")
			err = cmd.writeLibRefs(*librefsFilename, tilelib, librefs)
			if err != nil {
				return 1
			}
		}
	}
	log.WithFields(logrus.Fields{
		"rows": rows,
		"cols": cols,
	}).Info("writing numpy")
	npw.Shape = []int{rows, cols}
	npw.WriteInt16(out)
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

func (*exportNumpy) writeLibRefs(fnm string, tilelib *tileLibrary, librefs []tileLibRef) error {
	f, err := os.OpenFile(fnm, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	for i, libref := range librefs {
		_, err = fmt.Fprintf(f, "%d,%d,%d\n", i, libref.Tag, libref.Variant)
		if err != nil {
			return err
		}
	}
	return f.Close()
}

func cgs2array(tilelib *tileLibrary) (data []int16, rows, cols int, cgnames []string) {
	for name := range tilelib.compactGenomes {
		cgnames = append(cgnames, name)
	}
	sort.Slice(cgnames, func(i, j int) bool {
		return trimFilenameForLabel(cgnames[i]) < trimFilenameForLabel(cgnames[j])
	})

	rows = len(tilelib.compactGenomes)
	for _, cg := range tilelib.compactGenomes {
		if cols < len(cg) {
			cols = len(cg)
		}
	}

	// flag low-quality tile variants so we can change to -1 below
	lowqual := make([]map[tileVariantID]bool, cols/2)
	for tag, variants := range tilelib.variant {
		lq := lowqual[tag]
		for varidx, hash := range variants {
			if len(tilelib.seq[hash]) == 0 {
				if lq == nil {
					lq = map[tileVariantID]bool{}
					lowqual[tag] = lq
				}
				lq[tileVariantID(varidx+1)] = true
			}
		}
	}

	data = make([]int16, rows*cols)
	for row, name := range cgnames {
		for i, v := range tilelib.compactGenomes[name] {
			if v > 0 && lowqual[i/2][v] {
				data[row*cols+i] = -1
			} else {
				data[row*cols+i] = int16(v)
			}
		}
	}

	return
}

func recodeOnehot(in []int16, incols int) (out []int16, librefs []tileLibRef, outcols int) {
	rows := len(in) / incols
	maxvalue := make([]int16, incols)
	for row := 0; row < rows; row++ {
		for col := 0; col < incols; col++ {
			if v := in[row*incols+col]; maxvalue[col] < v {
				maxvalue[col] = v
			}
		}
	}
	outcol := make([]int, incols)
	dropped := 0
	for incol, maxv := range maxvalue {
		outcol[incol] = outcols
		if maxv == 0 {
			dropped++
		}
		for v := 1; v <= int(maxv); v++ {
			librefs = append(librefs, tileLibRef{Tag: tagID(incol), Variant: tileVariantID(v)})
			outcols++
		}
	}
	log.Printf("recodeOnehot: dropped %d input cols with zero maxvalue", dropped)

	out = make([]int16, rows*outcols)
	for inidx, row := 0, 0; row < rows; row++ {
		outrow := out[row*outcols:]
		for col := 0; col < incols; col++ {
			if v := in[inidx]; v > 0 {
				outrow[outcol[col]+int(v)-1] = 1
			}
			inidx++
		}
	}
	return
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

func trimFilenameForLabel(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	s = strings.TrimSuffix(s, ".gz")
	s = strings.TrimSuffix(s, ".fa")
	s = strings.TrimSuffix(s, ".fasta")
	s = strings.TrimSuffix(s, ".1")
	s = strings.TrimSuffix(s, ".2")
	s = strings.TrimSuffix(s, ".gz")
	s = strings.TrimSuffix(s, ".vcf")
	return s
}
