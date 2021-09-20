// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
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
	ref := flags.String("ref", "", "reference name (if blank, choose last one that appears in input)")
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
			RAM:         250000000000,
			VCPUs:       32,
			Priority:    *priority,
			KeepCache:   2,
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
	var refseq map[string][]tileLibRef
	in0, err := open(infiles[0])
	if err != nil {
		return 1
	}
	taglen := -1
	DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
		if len(ent.TagSet) > 0 {
			taglen = len(ent.TagSet[0])
		}
		for _, cseq := range ent.CompactSequences {
			if cseq.Name == *ref || *ref == "" {
				refseq = cseq.TileSequences
			}
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
	if refseq == nil {
		err = fmt.Errorf("%s: reference sequence not found", infiles[0])
		return 1
	}
	if taglen < 0 {
		err = fmt.Errorf("tagset not found")
		return 1
	}
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
	type reftileinfo struct {
		variant  tileVariantID
		seqname  string // chr1
		pos      int    // distance from start of chr1 to start of tile
		tiledata []byte // acgtggcaa...
	}
	reftile := map[tagID]*reftileinfo{}
	for seqname, cseq := range refseq {
		for _, libref := range cseq {
			reftile[libref.Tag] = &reftileinfo{seqname: seqname, variant: libref.Variant}
		}
	}
	log.Info("loading reference tiles from all slices")
	throttle := throttle{Max: runtime.GOMAXPROCS(0)}
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
					if dst, ok := reftile[tv.Tag]; ok && dst.variant == tv.Variant {
						dst.tiledata = tv.Sequence
					}
				}
				return nil
			})
		})
	}
	throttle.Wait()

	log.Info("reconstructing reference sequences")
	for seqname, cseq := range refseq {
		seqname, cseq := seqname, cseq
		throttle.Go(func() error {
			defer log.Printf("... %s done", seqname)
			pos := 0
			for _, libref := range cseq {
				rt := reftile[libref.Tag]
				rt.pos = pos
				pos += len(rt.tiledata) - taglen
			}
			return nil
		})
	}
	throttle.Wait()

	log.Info("TODO: determining which tiles intersect given regions")

	log.Info("generating annotations and numpy matrix for each slice")
	var done int64
	for infileIdx, infile := range infiles {
		infileIdx, infile := infileIdx, infile
		throttle.Go(func() error {
			seq := make(map[tagID][]TileVariant, 50000)
			cgs := make(map[string]CompactGenome, len(cgnames))
			f, err := open(infile)
			if err != nil {
				return err
			}
			defer f.Close()
			log.Infof("reading %s", infile)
			err = DecodeLibrary(f, strings.HasSuffix(infile, ".gz"), func(ent *LibraryEntry) error {
				for _, tv := range ent.TileVariants {
					variants := seq[tv.Tag]
					for len(variants) <= int(tv.Variant) {
						variants = append(variants, TileVariant{})
					}
					variants[int(tv.Variant)] = tv
					seq[tv.Tag] = variants
				}
				for _, cg := range ent.CompactGenomes {
					cgs[cg.Name] = cg
				}
				return nil
			})
			if err != nil {
				return err
			}
			tagstart := cgs[cgnames[0]].StartTag
			tagend := cgs[cgnames[0]].EndTag

			// TODO: filters

			log.Infof("renumbering variants for tags %d-%d", tagstart, tagend)
			variantRemap := make([][]tileVariantID, tagend-tagstart)
			var wg sync.WaitGroup
			for tag, variants := range seq {
				tag, variants := tag, variants
				wg.Add(1)
				go func() {
					defer wg.Done()
					count := make([]tileVariantID, len(variants))
					for _, cg := range cgs {
						idx := (tag - tagstart) * 2
						if int(idx) < len(cg.Variants) {
							count[cg.Variants[idx]]++
							count[cg.Variants[idx+1]]++
						}
					}
					ranked := make([]tileVariantID, len(variants))
					for i := range ranked {
						ranked[i] = tileVariantID(i)
					}
					sort.Slice(ranked, func(i, j int) bool {
						if i == 0 {
							return true // leave ranked[0] at position 0, unused
						} else if j == 0 {
							return false // ditto
						}
						if cri, crj := count[ranked[i]], count[ranked[j]]; cri != crj {
							return cri > crj
						} else {
							return bytes.Compare(variants[ranked[i]].Blake2b[:], variants[ranked[j]].Blake2b[:]) < 0
						}
					})
					remap := make([]tileVariantID, len(ranked))
					for i, r := range ranked {
						remap[r] = tileVariantID(i)
					}
					variantRemap[tag-tagstart] = remap
				}()
			}
			wg.Wait()

			annotationsFilename := fmt.Sprintf("%s/matrix.%04d.annotations.csv", *outputDir, infileIdx)
			log.Infof("writing %s", annotationsFilename)
			annof, err := os.Create(annotationsFilename)
			if err != nil {
				return err
			}
			annow := bufio.NewWriterSize(annof, 1<<20)
			for tag, variants := range seq {
				rt, ok := reftile[tag]
				if !ok {
					// Reference does not use any
					// variant of this tile.
					// TODO: log this? mention it
					// in annotations?
					continue
				}
				outcol := tag - tagID(tagstart)
				reftilestr := strings.ToUpper(string(rt.tiledata))
				remap := variantRemap[tag-tagstart]
				for v, tv := range variants {
					if len(tv.Sequence) < taglen || !bytes.HasSuffix(rt.tiledata, tv.Sequence[len(tv.Sequence)-taglen:]) {
						continue
					}
					if lendiff := len(rt.tiledata) - len(tv.Sequence); lendiff < -1000 || lendiff > 1000 {
						continue
					}
					diffs, _ := hgvs.Diff(reftilestr, strings.ToUpper(string(tv.Sequence)), 0)
					for _, diff := range diffs {
						diff.Position += rt.pos
						fmt.Fprintf(annow, "%d,%d,%d,%s:g.%s\n", tag, outcol, remap[v], rt.seqname, diff.String())
					}
				}
			}
			err = annow.Flush()
			if err != nil {
				return err
			}
			err = annof.Close()
			if err != nil {
				return err
			}

			log.Infof("%s: preparing numpy", infile)
			rows := len(cgnames)
			cols := 2 * int(tagend-tagstart)
			out := make([]int16, rows*cols)
			for row, name := range cgnames {
				out := out[row*cols:]
				for col, v := range cgs[name].Variants {
					if variants, ok := seq[tagstart+tagID(col/2)]; ok && len(variants) > int(v) && len(variants[v].Sequence) > 0 {
						out[col] = int16(variantRemap[col/2][v])
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
			bufw := bufio.NewWriterSize(output, 1<<26)
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
			log.Infof("%s: done (%d/%d)", infile, int(atomic.AddInt64(&done, 1)), len(infiles))
			return nil
		})
	}
	if err = throttle.Wait(); err != nil {
		return 1
	}
	return 0
}
