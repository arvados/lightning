// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
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
	inputDir := flags.String("input-dir", "./in", "input `directory`")
	outputDir := flags.String("output-dir", "./out", "output `directory`")
	annotationsFilename := flags.String("output-annotations", "", "output `file` for tile variant annotations csv")
	librefsFilename := flags.String("output-onehot2tilevar", "", "when using -one-hot, create csv `file` mapping column# to tag# and variant#")
	labelsFilename := flags.String("output-labels", "", "output `file` for genome labels csv")
	regionsFilename := flags.String("regions", "", "only output columns/annotations that intersect regions in specified bed `file`")
	expandRegions := flags.Int("expand-regions", 0, "expand specified regions by `N` base pairs on each side`")
	onehot := flags.Bool("one-hot", false, "recode tile variants as one-hot")
	chunks := flags.Int("chunks", 1, "split output into `N` numpy files")
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
			Name:        "lightning export-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         500000000000,
			VCPUs:       96,
			Priority:    *priority,
			KeepCache:   1,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir, regionsFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"export-numpy", "-local=true",
			"-pprof", ":6060",
			fmt.Sprintf("-one-hot=%v", *onehot),
			"-input-dir", *inputDir,
			"-output-dir", "/mnt/output",
			"-output-annotations", "/mnt/output/annotations.csv",
			"-output-onehot2tilevar", "/mnt/output/onehot2tilevar.csv",
			"-output-labels", "/mnt/output/labels.csv",
			"-regions", *regionsFilename,
			"-expand-regions", fmt.Sprintf("%d", *expandRegions),
			"-chunks", fmt.Sprintf("%d", *chunks),
		}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/matrix.npy")
		return 0
	}

	tilelib := &tileLibrary{
		retainNoCalls:       true,
		retainTileSequences: true,
		compactGenomes:      map[string][]tileVariantID{},
	}
	err = tilelib.LoadDir(context.Background(), *inputDir)
	if err != nil {
		return 1
	}

	log.Info("filtering")
	cmd.filter.Apply(tilelib)
	log.Info("tidying")
	tilelib.Tidy()

	log.Info("building lowqual map")
	lowqual := lowqual(tilelib)
	names := cgnames(tilelib)

	if *labelsFilename != "" {
		log.Infof("writing labels to %s", *labelsFilename)
		var f *os.File
		f, err = os.OpenFile(*labelsFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer f.Close()
		outBasename := "matrix.npy"
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

	log.Info("determining which tiles intersect given regions")
	dropTiles, err := chooseTiles(tilelib, *regionsFilename, *expandRegions)
	if err != nil {
		return 1
	}

	annotation2tvs := map[string]map[hgvs.Variant][]tileLibRef{}
	if *annotationsFilename != "" {
		log.Info("writing annotations")
		var annow io.WriteCloser
		annow, err = os.OpenFile(*annotationsFilename, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
		defer annow.Close()
		var mtx sync.Mutex
		err = (&annotatecmd{
			maxTileSize: 5000,
			dropTiles:   dropTiles,
			reportAnnotation: func(tag tagID, _ int, variant tileVariantID, refname string, seqname string, pdi hgvs.Variant) {
				mtx.Lock()
				defer mtx.Unlock()
				if annotation2tvs[seqname] == nil {
					annotation2tvs[seqname] = map[hgvs.Variant][]tileLibRef{}
				}
				annotation2tvs[seqname][pdi] = append(annotation2tvs[seqname][pdi], tileLibRef{Tag: tag, Variant: variant})
			},
		}).exportTileDiffs(annow, tilelib)
		if err != nil {
			return 1
		}
		err = annow.Close()
		if err != nil {
			return 1
		}
	}

	var lastErr atomic.Value
	var wg sync.WaitGroup
	for seqname, pdivars := range annotation2tvs {
		seqname, pdivars := seqname, pdivars
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("choosing hgvs columns for seq %s", seqname)
			var pdis []hgvs.Variant
			for pdi, librefs := range pdivars {
				// Include this HGVS column if it was
				// seen in a variant of any
				// non-dropped tile.
				for _, libref := range librefs {
					if int(libref.Tag) >= len(dropTiles) || !dropTiles[libref.Tag] {
						pdis = append(pdis, pdi)
						break
					}
				}
			}
			sort.Slice(pdis, func(i, j int) bool {
				if cmp := pdis[i].Position - pdis[j].Position; cmp != 0 {
					return cmp < 0
				} else if pdis[i].Ref != pdis[j].Ref {
					return pdis[i].Ref < pdis[j].Ref
				} else {
					return pdis[i].New < pdis[j].New
				}
			})
			log.Infof("writing column labels for seq %s", seqname)
			var buf bytes.Buffer
			for _, pdi := range pdis {
				fmt.Fprintf(&buf, "%s:g.%s\n", seqname, pdi.String())
			}
			err := ioutil.WriteFile(*outputDir+"/"+seqname+".columns.csv", buf.Bytes(), 0777)
			if err != nil {
				lastErr.Store(err)
				return
			}

			log.Infof("building hgvs matrix for seq %s", seqname)
			data := make([]int8, len(names)*len(pdis)*2)
			for row, name := range names {
				cg := tilelib.compactGenomes[name]
				rowstart := row * len(pdis) * 2
				for col, pdi := range pdis {
					for _, libref := range pdivars[pdi] {
						if len(cg) <= int(libref.Tag)*2+1 {
							continue
						}
						for phase := 0; phase < 2; phase++ {
							if cg[int(libref.Tag)*2+phase] == libref.Variant {
								data[rowstart+col*2+phase] = 1
							}
						}
					}
				}
			}
			log.Infof("writing hgvs numpy for seq %s", seqname)
			f, err := os.OpenFile(*outputDir+"/"+seqname+".npy", os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				lastErr.Store(err)
				return
			}
			defer f.Close()
			// gonpy closes our writer and ignores errors. Give it a nopCloser so we can close f properly.
			npw, err := gonpy.NewWriter(nopCloser{f})
			if err != nil {
				lastErr.Store(err)
				return
			}
			npw.Shape = []int{len(names), len(pdis) * 2}
			err = npw.WriteInt8(data)
			if err != nil {
				lastErr.Store(err)
				return
			}
			err = f.Close()
			if err != nil {
				lastErr.Store(err)
				return
			}
		}()
	}
	wg.Wait()
	if e, ok := lastErr.Load().(error); ok {
		err = e
		return 1
	}

	chunksize := (len(tilelib.variant) + *chunks - 1) / *chunks
	for chunk := 0; chunk < *chunks; chunk++ {
		log.Infof("preparing chunk %d of %d", chunk+1, *chunks)
		tagstart := chunk * chunksize
		tagend := tagstart + chunksize
		if tagend > len(tilelib.variant) {
			tagend = len(tilelib.variant)
		}
		out, rows, cols := cgs2array(tilelib, names, lowqual, dropTiles, tagstart, tagend)

		var npw *gonpy.NpyWriter
		var output io.WriteCloser
		fnm := *outputDir + "/matrix.npy"
		if *chunks > 1 {
			fnm = fmt.Sprintf("%s/matrix.%d.npy", *outputDir, chunk)
		}
		output, err = os.OpenFile(fnm, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer output.Close()
		bufw := bufio.NewWriter(output)
		npw, err = gonpy.NewWriter(nopCloser{bufw})
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
			"filename": fnm,
			"rows":     rows,
			"cols":     cols,
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

func cgnames(tilelib *tileLibrary) (cgnames []string) {
	for name := range tilelib.compactGenomes {
		cgnames = append(cgnames, name)
	}
	sort.Slice(cgnames, func(i, j int) bool {
		return trimFilenameForLabel(cgnames[i]) < trimFilenameForLabel(cgnames[j])
	})
	return
}

func lowqual(tilelib *tileLibrary) (lowqual []map[tileVariantID]bool) {
	lowqual = make([]map[tileVariantID]bool, len(tilelib.variant))
	for tag, variants := range tilelib.variant {
		lq := lowqual[tag]
		for varidx, hash := range variants {
			if len(tilelib.hashSequence(hash)) == 0 {
				if lq == nil {
					lq = map[tileVariantID]bool{}
					lowqual[tag] = lq
				}
				lq[tileVariantID(varidx+1)] = true
			}
		}
	}
	return
}

func cgs2array(tilelib *tileLibrary, names []string, lowqual []map[tileVariantID]bool, dropTiles []bool, tagstart, tagend int) (data []int16, rows, cols int) {
	rows = len(tilelib.compactGenomes)
	for tag := tagstart; tag < tagend; tag++ {
		if len(dropTiles) <= tag || !dropTiles[tag] {
			cols += 2
		}
	}
	data = make([]int16, rows*cols)
	for row, name := range names {
		cg := tilelib.compactGenomes[name]
		outidx := 0
		for tag := tagstart; tag < tagend && tag*2+1 < len(cg); tag++ {
			if len(dropTiles) > tag && dropTiles[tag] {
				continue
			}
			for phase := 0; phase < 2; phase++ {
				v := cg[tag*2+phase]
				if v > 0 && lowqual[tag][v] {
					data[row*cols+outidx] = -1
				} else {
					data[row*cols+outidx] = int16(v)
				}
				outidx++
			}
		}
	}
	return
}

func makeMask(regionsFilename string, expandRegions int) (*mask, error) {
	log.Printf("makeMask: reading %s", regionsFilename)
	rfile, err := zopen(regionsFilename)
	if err != nil {
		return nil, err
	}
	defer rfile.Close()
	regions, err := io.ReadAll(rfile)
	if err != nil {
		return nil, err
	}

	log.Print("makeMask: building mask")
	var mask mask
	for _, line := range bytes.Split(regions, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte{'#'}) {
			continue
		}
		fields := bytes.Split(line, []byte{'\t'})
		if len(fields) < 3 {
			continue
		}
		refseqname := string(fields[0])
		if strings.HasPrefix(refseqname, "chr") {
			refseqname = refseqname[3:]
		}
		start, err1 := strconv.Atoi(string(fields[1]))
		end, err2 := strconv.Atoi(string(fields[2]))
		if err1 == nil && err2 == nil {
			// BED
		} else {
			start, err1 = strconv.Atoi(string(fields[3]))
			end, err2 = strconv.Atoi(string(fields[4]))
			if err1 == nil && err2 == nil {
				// GFF/GTF
				end++
			} else {
				return nil, fmt.Errorf("cannot parse input line as BED or GFF/GTF: %q", line)
			}
		}
		mask.Add(refseqname, start-expandRegions, end+expandRegions)
	}
	log.Print("makeMask: mask.Freeze")
	mask.Freeze()
	return &mask, nil
}

func chooseTiles(tilelib *tileLibrary, regionsFilename string, expandRegions int) (drop []bool, err error) {
	if regionsFilename == "" {
		return
	}
	mask, err := makeMask(regionsFilename, expandRegions)
	if err != nil {
		return
	}

	tagset := tilelib.taglib.Tags()
	if len(tagset) == 0 {
		err = errors.New("cannot choose tiles by region in a library without tags")
		return
	}
	taglen := len(tagset[0])

	log.Print("chooseTiles: check ref tiles")
	// Find position+size of each reference tile, and if it
	// intersects any of the desired regions, set drop[tag]=false.
	//
	// (Note it doesn't quite work to do the more obvious thing --
	// start with drop=false and change to true when ref tiles
	// intersect target regions -- because that would give us
	// drop=false for tiles that don't appear at all in the
	// reference.)
	//
	// TODO: (optionally?) don't drop tags for which some tile
	// variants are spanning tiles, i.e., where the reference tile
	// does not intersect the desired regions, but a spanning tile
	// from a genome does.
	drop = make([]bool, len(tilelib.variant))
	for i := range drop {
		drop[i] = true
	}
	for refname, refseqs := range tilelib.refseqs {
		for refseqname, reftiles := range refseqs {
			if strings.HasPrefix(refseqname, "chr") {
				refseqname = refseqname[3:]
			}
			tileend := 0
			for _, libref := range reftiles {
				if libref.Variant < 1 {
					err = fmt.Errorf("reference %q seq %q uses variant zero at tag %d", refname, refseqname, libref.Tag)
					return
				}
				seq := tilelib.TileVariantSequence(libref)
				if len(seq) < taglen {
					err = fmt.Errorf("reference %q seq %q uses tile %d variant %d with sequence len %d < taglen %d", refname, refseqname, libref.Tag, libref.Variant, len(seq), taglen)
					return
				}
				tilestart := tileend
				tileend = tilestart + len(seq) - taglen
				if mask.Check(refseqname, tilestart, tileend) {
					drop[libref.Tag] = false
				}
			}
		}
	}

	log.Print("chooseTiles: done")
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
