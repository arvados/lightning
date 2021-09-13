// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/kshedden/gonpy"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type sliceNumpy struct {
	filter filter
}

func (cmd *sliceNumpy) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	inputDir := flags.String("input-dir", "./in", "input `directory`")
	outputDir := flags.String("output-dir", "./out", "output `directory`")
	regionsFilename := flags.String("regions", "", "only output columns/annotations that intersect regions in specified bed `file`")
	expandRegions := flags.Int("expand-regions", 0, "expand specified regions by `N` base pairs on each side`")
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
		runner := arvadosContainerRunner{
			Name:        "lightning slice-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         150000000000,
			VCPUs:       32,
			Priority:    *priority,
			KeepCache:   1,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir, regionsFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"slice-numpy", "-local=true",
			"-pprof", ":6060",
			"-input-dir", *inputDir,
			"-output-dir", "/mnt/output",
			"-regions", *regionsFilename,
			"-expand-regions", fmt.Sprintf("%d", *expandRegions),
		}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output)
		return 0
	}

	infiles, err := allGobFiles(*inputDir)
	if err != nil {
		return 1
	}
	if len(infiles) == 0 {
		err = fmt.Errorf("no input files found in %s", *inputDir)
		return 1
	}
	sort.Strings(infiles)

	var cgnames []string
	refseqs := map[string]map[string][]tileLibRef{}
	in0, err := open(infiles[0])
	if err != nil {
		return 1
	}
	DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
		for _, cseq := range ent.CompactSequences {
			refseqs[cseq.Name] = cseq.TileSequences
		}
		for _, cg := range ent.CompactGenomes {
			cgnames = append(cgnames, cg.Name)
		}
		return nil
	})
	if err != nil {
		return 1
	}
	in0.Close()
	sort.Strings(cgnames)

	{
		labelsFilename := *outputDir + "/labels.csv"
		log.Infof("writing labels to %s", labelsFilename)
		var f *os.File
		f, err = os.Create(labelsFilename)
		if err != nil {
			return 1
		}
		defer f.Close()
		for i, name := range cgnames {
			_, err = fmt.Fprintf(f, "%d,%q\n", i, trimFilenameForLabel(name))
			if err != nil {
				err = fmt.Errorf("write %s: %w", labelsFilename, err)
				return 1
			}
		}
		err = f.Close()
		if err != nil {
			err = fmt.Errorf("close %s: %w", labelsFilename, err)
			return 1
		}
	}

	log.Info("building list of reference tiles to load") // TODO: more efficient if we had saved all ref tiles in slice0
	reftiledata := map[tileLibRef]*[]byte{}
	for _, ref := range refseqs {
		for _, cseq := range ref {
			for _, libref := range cseq {
				reftiledata[libref] = new([]byte)
			}
		}
	}
	log.Info("loading reference tiles from all slices")
	throttle := throttle{Max: runtime.NumCPU()}
	for _, infile := range infiles {
		infile := infile
		throttle.Go(func() error {
			defer log.Infof("%s: done", infile)
			f, err := open(infile)
			if err != nil {
				return err
			}
			defer f.Close()
			return DecodeLibrary(f, strings.HasSuffix(infile, ".gz"), func(ent *LibraryEntry) error {
				for _, tv := range ent.TileVariants {
					libref := tileLibRef{tv.Tag, tv.Variant}
					if dst, ok := reftiledata[libref]; ok {
						*dst = tv.Sequence
					}
				}
				return nil
			})
		})
	}
	throttle.Wait()

	log.Info("TODO: determining which tiles intersect given regions")

	log.Info("generating annotations and numpy matrix for each slice")
	for infileIdx, infile := range infiles {
		infileIdx, infile := infileIdx, infile
		throttle.Go(func() error {
			defer log.Infof("%s: done", infile)
			seq := map[tileLibRef][]byte{}
			cgs := make(map[string]CompactGenome, len(cgnames))
			f, err := open(infile)
			if err != nil {
				return err
			}
			defer f.Close()
			err = DecodeLibrary(f, strings.HasSuffix(infile, ".gz"), func(ent *LibraryEntry) error {
				for _, tv := range ent.TileVariants {
					seq[tileLibRef{tv.Tag, tv.Variant}] = tv.Sequence
				}
				for _, cg := range ent.CompactGenomes {
					cgs[cg.Name] = cg
				}
				return nil
			})
			if err != nil {
				return err
			}

			log.Infof("TODO: %s: filtering", infile)
			log.Infof("TODO: %s: tidying", infile)
			log.Infof("TODO: %s: lowqual to -1", infile)

			annotationsFilename := fmt.Sprintf("%s/matrix.%04d.annotations.csv", *outputDir, infileIdx)
			log.Infof("%s: writing annotations to %s", infile, annotationsFilename)
			annow, err := os.Create(annotationsFilename)
			if err != nil {
				return err
			}
			// for libref, seq := range seq {
			// 	// TODO: diff from ref.
			// }
			err = annow.Close()
			if err != nil {
				return err
			}

			log.Infof("%s: preparing numpy", infile)
			tagstart := cgs[cgnames[0]].StartTag
			tagend := cgs[cgnames[0]].EndTag
			rows := len(cgnames)
			cols := 2 * int(tagend-tagstart)
			out := make([]int16, rows*cols)
			for row, name := range cgnames {
				out := out[row*cols:]
				for col, v := range cgs[name].Variants {
					if v == 0 {
						// out[col] = 0
					} else if _, ok := seq[tileLibRef{tagstart + tagID(col/2), v}]; ok {
						out[col] = int16(v)
					} else {
						out[col] = -1
					}
				}
			}

			fnm := fmt.Sprintf("%s/matrix.%04d.npy", *outputDir, infileIdx)
			output, err := os.Create(fnm)
			if err != nil {
				return err
			}
			defer output.Close()
			bufw := bufio.NewWriter(output)
			npw, err := gonpy.NewWriter(nopCloser{bufw})
			if err != nil {
				return err
			}
			log.WithFields(logrus.Fields{
				"filename": fnm,
				"rows":     rows,
				"cols":     cols,
			}).Info("writing numpy")
			npw.Shape = []int{rows, cols}
			npw.WriteInt16(out)
			err = bufw.Flush()
			if err != nil {
				return err
			}
			err = output.Close()
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err = throttle.Wait(); err != nil {
		return 1
	}
	return 0
}

func (*sliceNumpy) writeLibRefs(fnm string, tilelib *tileLibrary, librefs []tileLibRef) error {
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
