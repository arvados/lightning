// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
	"github.com/kshedden/gonpy"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type sliceNumpy struct {
	filter        filter
	threads       int
	chi2CasesFile string
	chi2Cases     []bool
	chi2PValue    float64
	minCoverage   int
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
	mergeOutput := flags.Bool("merge-output", false, "merge output into one matrix.npy and one matrix.annotations.csv")
	hgvsSingle := flags.Bool("single-hgvs-matrix", false, "also generate hgvs-based matrix")
	hgvsChunked := flags.Bool("chunked-hgvs-matrix", false, "also generate hgvs-based matrix per chromosome")
	flags.IntVar(&cmd.threads, "threads", 16, "number of memory-hungry assembly threads")
	flags.StringVar(&cmd.chi2CasesFile, "chi2-cases-file", "", "text file indicating positive cases (for Χ² test)")
	flags.Float64Var(&cmd.chi2PValue, "chi2-p-value", 1, "do Χ² test and omit columns with p-value above this threshold")
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

	if cmd.chi2CasesFile == "" && cmd.chi2PValue != 1 {
		log.Errorf("cannot use provided -chi2-p-value=%f because -chi2-cases-file= value is empty", cmd.chi2PValue)
		return 2
	}

	if !*runlocal {
		runner := arvadosContainerRunner{
			Name:        "lightning slice-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         750000000000,
			VCPUs:       96,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir, regionsFilename, &cmd.chi2CasesFile)
		if err != nil {
			return 1
		}
		runner.Args = []string{"slice-numpy", "-local=true",
			"-pprof=:6060",
			"-input-dir=" + *inputDir,
			"-output-dir=/mnt/output",
			"-threads=" + fmt.Sprintf("%d", cmd.threads),
			"-regions=" + *regionsFilename,
			"-expand-regions=" + fmt.Sprintf("%d", *expandRegions),
			"-merge-output=" + fmt.Sprintf("%v", *mergeOutput),
			"-single-hgvs-matrix=" + fmt.Sprintf("%v", *hgvsSingle),
			"-chunked-hgvs-matrix=" + fmt.Sprintf("%v", *hgvsChunked),
			"-chi2-cases-file=" + cmd.chi2CasesFile,
			"-chi2-p-value=" + fmt.Sprintf("%f", cmd.chi2PValue),
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
	var reftiledata = make(map[tileLibRef][]byte, 11000000)
	in0, err := open(infiles[0])
	if err != nil {
		return 1
	}

	matchGenome, err := regexp.Compile(cmd.filter.MatchGenome)
	if err != nil {
		err = fmt.Errorf("-match-genome: invalid regexp: %q", cmd.filter.MatchGenome)
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
			if matchGenome.MatchString(cg.Name) {
				cgnames = append(cgnames, cg.Name)
			}
		}
		for _, tv := range ent.TileVariants {
			if tv.Ref {
				reftiledata[tileLibRef{tv.Tag, tv.Variant}] = tv.Sequence
			}
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
	if len(cgnames) == 0 {
		err = fmt.Errorf("no genomes found matching regexp %q", cmd.filter.MatchGenome)
		return 1
	}
	sort.Strings(cgnames)

	cmd.minCoverage = int(math.Ceil(cmd.filter.MinCoverage * float64(len(cgnames))))

	if cmd.chi2CasesFile != "" {
		f, err2 := open(cmd.chi2CasesFile)
		if err2 != nil {
			err = err2
			return 1
		}
		buf, err2 := io.ReadAll(f)
		f.Close()
		if err2 != nil {
			err = err2
			return 1
		}
		cmd.chi2Cases = make([]bool, len(cgnames))
		ncases := 0
		for _, pattern := range bytes.Split(buf, []byte{'\n'}) {
			if len(pattern) == 0 {
				continue
			}
			pattern := string(pattern)
			idx := -1
			for i, name := range cgnames {
				if !strings.Contains(name, pattern) {
					continue
				}
				cmd.chi2Cases[i] = true
				ncases++
				if idx >= 0 {
					log.Warnf("pattern %q in cases file matches multiple genome IDs: %q, %q", pattern, cgnames[idx], name)
				} else {
					idx = i
				}
			}
			if idx < 0 {
				log.Warnf("pattern %q in cases file does not match any genome IDs", pattern)
				continue
			}
		}
		log.Printf("%d cases, %d controls", ncases, len(cgnames)-ncases)
	}

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

	log.Info("indexing reference tiles")
	type reftileinfo struct {
		variant  tileVariantID
		seqname  string // chr1
		pos      int    // distance from start of chromosome to starttag
		tiledata []byte // acgtggcaa...
	}
	isdup := map[tagID]bool{}
	reftile := map[tagID]*reftileinfo{}
	for seqname, cseq := range refseq {
		pos := 0
		for _, libref := range cseq {
			tiledata := reftiledata[libref]
			if len(tiledata) == 0 {
				err = fmt.Errorf("missing tiledata for tag %d variant %d in %s in ref", libref.Tag, libref.Variant, seqname)
				return 1
			}
			if isdup[libref.Tag] {
				log.Printf("dropping reference tile %+v from %s @ %d, tag not unique", libref, seqname, pos)
			} else if reftile[libref.Tag] != nil {
				log.Printf("dropping reference tile %+v from %s @ %d, tag not unique", tileLibRef{Tag: libref.Tag, Variant: reftile[libref.Tag].variant}, reftile[libref.Tag].seqname, reftile[libref.Tag].pos)
				delete(reftile, libref.Tag)
				log.Printf("dropping reference tile %+v from %s @ %d, tag not unique", libref, seqname, pos)
				isdup[libref.Tag] = true
			} else {
				reftile[libref.Tag] = &reftileinfo{
					seqname:  seqname,
					variant:  libref.Variant,
					tiledata: tiledata,
					pos:      pos,
				}
			}
			pos += len(tiledata) - taglen
		}
		log.Printf("... %s done, len %d", seqname, pos+taglen)
	}

	var mask *mask
	if *regionsFilename != "" {
		log.Printf("loading regions from %s", *regionsFilename)
		mask, err = makeMask(*regionsFilename, *expandRegions)
		if err != nil {
			return 1
		}
		log.Printf("before applying mask, len(reftile) == %d", len(reftile))
		log.Printf("deleting reftile entries for regions outside %d intervals", mask.Len())
		for tag, rt := range reftile {
			if !mask.Check(strings.TrimPrefix(rt.seqname, "chr"), rt.pos, rt.pos+len(rt.tiledata)) {
				delete(reftile, tag)
			}
		}
		log.Printf("after applying mask, len(reftile) == %d", len(reftile))
	}

	type hgvsColSet map[hgvs.Variant][2][]int8
	encodeHGVS := throttle{Max: len(refseq)}
	encodeHGVSTodo := map[string]chan hgvsColSet{}
	tmpHGVSCols := map[string]*os.File{}
	if *hgvsChunked {
		for seqname := range refseq {
			var f *os.File
			f, err = os.Create(*outputDir + "/tmp." + seqname + ".gob")
			if err != nil {
				return 1
			}
			defer os.Remove(f.Name())
			bufw := bufio.NewWriterSize(f, 1<<24)
			enc := gob.NewEncoder(bufw)
			tmpHGVSCols[seqname] = f
			todo := make(chan hgvsColSet, 128)
			encodeHGVSTodo[seqname] = todo
			encodeHGVS.Go(func() error {
				for colset := range todo {
					err := enc.Encode(colset)
					if err != nil {
						encodeHGVS.Report(err)
						for range todo {
						}
						return err
					}
				}
				return bufw.Flush()
			})
		}
	}

	var toMerge [][]int16
	if *mergeOutput || *hgvsSingle {
		toMerge = make([][]int16, len(infiles))
	}

	throttleMem := throttle{Max: cmd.threads} // TODO: estimate using mem and data size
	throttleNumpyMem := throttle{Max: cmd.threads/2 + 1}
	log.Info("generating annotations and numpy matrix for each slice")
	var done int64
	for infileIdx, infile := range infiles {
		infileIdx, infile := infileIdx, infile
		throttleMem.Go(func() error {
			seq := make(map[tagID][]TileVariant, 50000)
			cgs := make(map[string]CompactGenome, len(cgnames))
			f, err := open(infile)
			if err != nil {
				return err
			}
			defer f.Close()
			log.Infof("%04d: reading %s", infileIdx, infile)
			err = DecodeLibrary(f, strings.HasSuffix(infile, ".gz"), func(ent *LibraryEntry) error {
				for _, tv := range ent.TileVariants {
					if tv.Ref {
						continue
					}
					if mask != nil && reftile[tv.Tag] == nil {
						// Don't waste
						// time/memory on
						// masked-out tiles.
						continue
					}
					variants := seq[tv.Tag]
					if len(variants) == 0 {
						variants = make([]TileVariant, 100)
					}
					for len(variants) <= int(tv.Variant) {
						variants = append(variants, TileVariant{})
					}
					variants[int(tv.Variant)] = tv
					seq[tv.Tag] = variants
				}
				for _, cg := range ent.CompactGenomes {
					if !matchGenome.MatchString(cg.Name) {
						continue
					}
					// pad to full slice size
					// to avoid out-of-bounds
					// checks later
					if sliceSize := 2 * int(cg.EndTag-cg.StartTag); len(cg.Variants) < sliceSize {
						cg.Variants = append(cg.Variants, make([]tileVariantID, sliceSize-len(cg.Variants))...)
					}
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

			log.Infof("%04d: renumber/dedup variants for tags %d-%d", infileIdx, tagstart, tagend)
			variantRemap := make([][]tileVariantID, tagend-tagstart)
			throttleCPU := throttle{Max: runtime.GOMAXPROCS(0)}
			for tag, variants := range seq {
				tag, variants := tag, variants
				throttleCPU.Acquire()
				go func() {
					defer throttleCPU.Release()
					count := make(map[[blake2b.Size256]byte]int, len(variants))

					rt := reftile[tag]
					if rt != nil {
						count[blake2b.Sum256(rt.tiledata)] = 0
					}

					for _, cg := range cgs {
						idx := int(tag-tagstart) * 2
						for allele := 0; allele < 2; allele++ {
							v := cg.Variants[idx+allele]
							if v > 0 && len(variants[v].Sequence) > 0 {
								count[variants[v].Blake2b]++
							}
						}
					}
					// hash[i] will be the hash of
					// the variant(s) that should
					// be at rank i (0-based).
					hash := make([][blake2b.Size256]byte, 0, len(count))
					for b := range count {
						hash = append(hash, b)
					}
					sort.Slice(hash, func(i, j int) bool {
						bi, bj := &hash[i], &hash[j]
						if ci, cj := count[*bi], count[*bj]; ci != cj {
							return ci > cj
						} else {
							return bytes.Compare((*bi)[:], (*bj)[:]) < 0
						}
					})
					// rank[b] will be the 1-based
					// new variant number for
					// variants whose hash is b.
					rank := make(map[[blake2b.Size256]byte]tileVariantID, len(hash))
					for i, h := range hash {
						rank[h] = tileVariantID(i + 1)
					}
					// remap[v] will be the new
					// variant number for original
					// variant number v.
					remap := make([]tileVariantID, len(variants))
					for i, tv := range variants {
						remap[i] = rank[tv.Blake2b]
					}
					variantRemap[tag-tagstart] = remap
					if rt != nil {
						rt.variant = rank[blake2b.Sum256(rt.tiledata)]
					}
				}()
			}
			throttleCPU.Wait()

			annotationsFilename := fmt.Sprintf("%s/matrix.%04d.annotations.csv", *outputDir, infileIdx)
			log.Infof("%04d: writing %s", infileIdx, annotationsFilename)
			annof, err := os.Create(annotationsFilename)
			if err != nil {
				return err
			}
			annow := bufio.NewWriterSize(annof, 1<<20)
			outcol := 0
			for tag := tagstart; tag < tagend; tag++ {
				rt, ok := reftile[tag]
				if !ok {
					if mask == nil {
						outcol++
					}
					// Excluded by specified
					// regions, or reference does
					// not use any variant of this
					// tile. (TODO: log this?
					// mention it in annotations?)
					continue
				}
				fmt.Fprintf(annow, "%d,%d,%d,=,%s,%d,,,\n", tag, outcol, rt.variant, rt.seqname, rt.pos)
				variants := seq[tag]
				reftilestr := strings.ToUpper(string(rt.tiledata))
				remap := variantRemap[tag-tagstart]
				maxv := tileVariantID(0)
				for _, v := range remap {
					if maxv < v {
						maxv = v
					}
				}
				done := make([]bool, maxv+1)
				variantDiffs := make([][]hgvs.Variant, maxv+1)
				for v, tv := range variants {
					v := remap[v]
					if v == rt.variant || done[v] {
						continue
					} else {
						done[v] = true
					}
					if len(tv.Sequence) < taglen || !bytes.HasSuffix(rt.tiledata, tv.Sequence[len(tv.Sequence)-taglen:]) {
						fmt.Fprintf(annow, "%d,%d,%d,,%s,%d,,,\n", tag, outcol, v, rt.seqname, rt.pos)
						continue
					}
					if lendiff := len(rt.tiledata) - len(tv.Sequence); lendiff < -1000 || lendiff > 1000 {
						fmt.Fprintf(annow, "%d,%d,%d,,%s,%d,,,\n", tag, outcol, v, rt.seqname, rt.pos)
						continue
					}
					diffs, _ := hgvs.Diff(reftilestr, strings.ToUpper(string(tv.Sequence)), 0)
					for i := range diffs {
						diffs[i].Position += rt.pos
					}
					for _, diff := range diffs {
						fmt.Fprintf(annow, "%d,%d,%d,%s:g.%s,%s,%d,%s,%s,%s\n", tag, outcol, v, rt.seqname, diff.String(), rt.seqname, diff.Position, diff.Ref, diff.New, diff.Left)
					}
					if *hgvsChunked {
						variantDiffs[v] = diffs
					}
				}
				if *hgvsChunked {
					// We can now determine, for each HGVS
					// variant (diff) in this reftile
					// region, whether a given genome
					// phase/allele (1) has the variant, (0) has
					// =ref or a different variant in that
					// position, or (-1) is lacking
					// coverage / couldn't be diffed.
					hgvsCol := hgvsColSet{}
					for _, diffs := range variantDiffs {
						for _, diff := range diffs {
							if _, ok := hgvsCol[diff]; ok {
								continue
							}
							hgvsCol[diff] = [2][]int8{
								make([]int8, len(cgnames)),
								make([]int8, len(cgnames)),
							}
						}
					}
					for row, name := range cgnames {
						variants := cgs[name].Variants[(tag-tagstart)*2:]
						for ph := 0; ph < 2; ph++ {
							v := variants[ph]
							if int(v) >= len(remap) {
								v = 0
							} else {
								v = remap[v]
							}
							if v == rt.variant {
								// hgvsCol[*][ph][row] is already 0
							} else if len(variantDiffs[v]) == 0 {
								// lacking coverage / couldn't be diffed
								for _, col := range hgvsCol {
									col[ph][row] = -1
								}
							} else {
								for _, diff := range variantDiffs[v] {
									hgvsCol[diff][ph][row] = 1
								}
							}
						}
					}
					for diff, colpair := range hgvsCol {
						allele2homhet(colpair)
						if !cmd.filterHGVScolpair(colpair) {
							delete(hgvsCol, diff)
						}
					}
					if len(hgvsCol) > 0 {
						encodeHGVSTodo[rt.seqname] <- hgvsCol
					}
				}
				outcol++
			}
			err = annow.Flush()
			if err != nil {
				return err
			}
			err = annof.Close()
			if err != nil {
				return err
			}

			log.Infof("%04d: preparing numpy", infileIdx)
			throttleNumpyMem.Acquire()
			rows := len(cgnames)
			cols := 2 * outcol
			out := make([]int16, rows*cols)
			for row, name := range cgnames {
				out := out[row*cols:]
				outcol := 0
				for col, v := range cgs[name].Variants {
					tag := tagstart + tagID(col/2)
					if mask != nil && reftile[tag] == nil {
						continue
					}
					if variants, ok := seq[tag]; ok && len(variants) > int(v) && len(variants[v].Sequence) > 0 {
						out[outcol] = int16(variantRemap[tag-tagstart][v])
					} else {
						out[outcol] = -1
					}
					outcol++
				}
			}
			seq = nil
			cgs = nil
			debug.FreeOSMemory()
			throttleNumpyMem.Release()

			if *mergeOutput || *hgvsSingle {
				log.Infof("%04d: matrix fragment %d rows x %d cols", infileIdx, rows, cols)
				toMerge[infileIdx] = out
			}
			if !*mergeOutput {
				fnm := fmt.Sprintf("%s/matrix.%04d.npy", *outputDir, infileIdx)
				err = writeNumpyInt16(fnm, out, rows, cols)
				if err != nil {
					return err
				}
				debug.FreeOSMemory()
			}
			log.Infof("%s: done (%d/%d)", infile, int(atomic.AddInt64(&done, 1)), len(infiles))
			return nil
		})
	}
	if err = throttleMem.Wait(); err != nil {
		return 1
	}

	if *hgvsChunked {
		log.Info("flushing hgvsCols temp files")
		for seqname := range refseq {
			close(encodeHGVSTodo[seqname])
		}
		err = encodeHGVS.Wait()
		if err != nil {
			return 1
		}
		for seqname := range refseq {
			log.Infof("%s: reading hgvsCols from temp file", seqname)
			f := tmpHGVSCols[seqname]
			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				return 1
			}
			var hgvsCols hgvsColSet
			dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<24))
			for err == nil {
				err = dec.Decode(&hgvsCols)
			}
			if err != io.EOF {
				return 1
			}
			log.Infof("%s: sorting %d hgvs variants", seqname, len(hgvsCols))
			variants := make([]hgvs.Variant, 0, len(hgvsCols))
			for v := range hgvsCols {
				variants = append(variants, v)
			}
			sort.Slice(variants, func(i, j int) bool {
				vi, vj := &variants[i], &variants[j]
				if vi.Position != vj.Position {
					return vi.Position < vj.Position
				} else if vi.Ref != vj.Ref {
					return vi.Ref < vj.Ref
				} else {
					return vi.New < vj.New
				}
			})
			rows := len(cgnames)
			cols := len(variants) * 2
			log.Infof("%s: building hgvs matrix (rows=%d, cols=%d, mem=%d)", seqname, rows, cols, rows*cols)
			out := make([]int8, rows*cols)
			for varIdx, variant := range variants {
				hgvsCols := hgvsCols[variant]
				for row := range cgnames {
					for ph := 0; ph < 2; ph++ {
						out[row*cols+varIdx+ph] = hgvsCols[ph][row]
					}
				}
			}
			err = writeNumpyInt8(fmt.Sprintf("%s/hgvs.%s.npy", *outputDir, seqname), out, rows, cols)
			if err != nil {
				return 1
			}
			out = nil

			fnm := fmt.Sprintf("%s/hgvs.%s.annotations.csv", *outputDir, seqname)
			log.Infof("%s: writing hgvs column labels to %s", seqname, fnm)
			var hgvsLabels bytes.Buffer
			for varIdx, variant := range variants {
				fmt.Fprintf(&hgvsLabels, "%d,%s:g.%s\n", varIdx, seqname, variant.String())
			}
			err = ioutil.WriteFile(fnm, hgvsLabels.Bytes(), 0666)
			if err != nil {
				return 1
			}
		}
	}

	if *mergeOutput || *hgvsSingle {
		var annow *bufio.Writer
		var annof *os.File
		if *mergeOutput {
			annoFilename := fmt.Sprintf("%s/matrix.annotations.csv", *outputDir)
			annof, err = os.Create(annoFilename)
			if err != nil {
				return 1
			}
			annow = bufio.NewWriterSize(annof, 1<<20)
		}

		rows := len(cgnames)
		cols := 0
		for _, chunk := range toMerge {
			cols += len(chunk) / rows
		}
		log.Infof("merging output matrix (rows=%d, cols=%d, mem=%d) and annotations", rows, cols, rows*cols*2)
		var out []int16
		if *mergeOutput {
			out = make([]int16, rows*cols)
		}
		hgvsCols := map[string][2][]int16{} // hgvs -> [[g0,g1,g2,...], [g0,g1,g2,...]] (slice of genomes for each phase)
		startcol := 0
		for outIdx, chunk := range toMerge {
			chunkcols := len(chunk) / rows
			if *mergeOutput {
				for row := 0; row < rows; row++ {
					copy(out[row*cols+startcol:], chunk[row*chunkcols:(row+1)*chunkcols])
				}
			}
			toMerge[outIdx] = nil

			annotationsFilename := fmt.Sprintf("%s/matrix.%04d.annotations.csv", *outputDir, outIdx)
			log.Infof("reading %s", annotationsFilename)
			buf, err := os.ReadFile(annotationsFilename)
			if err != nil {
				return 1
			}
			if *mergeOutput {
				err = os.Remove(annotationsFilename)
				if err != nil {
					return 1
				}
			}
			for _, line := range bytes.Split(buf, []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				fields := bytes.SplitN(line, []byte{','}, 9)
				tag, _ := strconv.Atoi(string(fields[0]))
				incol, _ := strconv.Atoi(string(fields[1]))
				tileVariant, _ := strconv.Atoi(string(fields[2]))
				hgvsID := string(fields[3])
				seqname := string(fields[4])
				pos, _ := strconv.Atoi(string(fields[5]))
				refseq := fields[6]
				if hgvsID == "" {
					// Null entry for un-diffable
					// tile variant
					continue
				}
				if hgvsID == "=" {
					// Null entry for ref tile
					continue
				}
				if mask != nil && !mask.Check(strings.TrimPrefix(seqname, "chr"), pos, pos+len(refseq)) {
					// The tile intersects one of
					// the selected regions, but
					// this particular HGVS
					// variant does not.
					continue
				}
				hgvsColPair := hgvsCols[hgvsID]
				if hgvsColPair[0] == nil {
					// values in new columns start
					// out as -1 ("no data yet")
					// or 0 ("=ref") here, may
					// change to 1 ("hgvs variant
					// present") below, either on
					// this line or a future line.
					hgvsColPair = [2][]int16{make([]int16, len(cgnames)), make([]int16, len(cgnames))}
					rt, ok := reftile[tagID(tag)]
					if !ok {
						err = fmt.Errorf("bug: seeing annotations for tag %d, but it has no reftile entry", tag)
						return 1
					}
					for ph := 0; ph < 2; ph++ {
						for row := 0; row < rows; row++ {
							v := chunk[row*chunkcols+incol*2+ph]
							if tileVariantID(v) == rt.variant {
								hgvsColPair[ph][row] = 0
							} else {
								hgvsColPair[ph][row] = -1
							}
						}
					}
					hgvsCols[hgvsID] = hgvsColPair
					if annow != nil {
						hgvsref := hgvs.Variant{
							Position: pos,
							Ref:      string(refseq),
							New:      string(refseq),
						}
						fmt.Fprintf(annow, "%d,%d,%d,%s:g.%s,%s,%d,%s,%s,%s\n", tag, incol+startcol/2, rt.variant, seqname, hgvsref.String(), seqname, pos, refseq, refseq, fields[8])
					}
				}
				if annow != nil {
					fmt.Fprintf(annow, "%d,%d,%d,%s,%s,%d,%s,%s,%s\n", tag, incol+startcol/2, tileVariant, hgvsID, seqname, pos, refseq, fields[7], fields[8])
				}
				for ph := 0; ph < 2; ph++ {
					for row := 0; row < rows; row++ {
						v := chunk[row*chunkcols+incol*2+ph]
						if int(v) == tileVariant {
							hgvsColPair[ph][row] = 1
						}
					}
				}
			}

			startcol += chunkcols
		}
		if *mergeOutput {
			err = annow.Flush()
			if err != nil {
				return 1
			}
			err = annof.Close()
			if err != nil {
				return 1
			}
			err = writeNumpyInt16(fmt.Sprintf("%s/matrix.npy", *outputDir), out, rows, cols)
			if err != nil {
				return 1
			}
		}
		out = nil

		if *hgvsSingle {
			cols = len(hgvsCols) * 2
			log.Printf("building hgvs-based matrix: %d rows x %d cols", rows, cols)
			out = make([]int16, rows*cols)
			hgvsIDs := make([]string, 0, cols/2)
			for hgvsID := range hgvsCols {
				hgvsIDs = append(hgvsIDs, hgvsID)
			}
			sort.Strings(hgvsIDs)
			var hgvsLabels bytes.Buffer
			for idx, hgvsID := range hgvsIDs {
				fmt.Fprintf(&hgvsLabels, "%d,%s\n", idx, hgvsID)
				for ph := 0; ph < 2; ph++ {
					hgvscol := hgvsCols[hgvsID][ph]
					for row, val := range hgvscol {
						out[row*cols+idx*2+ph] = val
					}
				}
			}
			err = writeNumpyInt16(fmt.Sprintf("%s/hgvs.npy", *outputDir), out, rows, cols)
			if err != nil {
				return 1
			}

			fnm := fmt.Sprintf("%s/hgvs.annotations.csv", *outputDir)
			log.Printf("writing hgvs labels: %s", fnm)
			err = ioutil.WriteFile(fnm, hgvsLabels.Bytes(), 0777)
			if err != nil {
				return 1
			}
		}
	}
	return 0
}

func (cmd *sliceNumpy) filterHGVScolpair(colpair [2][]int8) bool {
	if cmd.chi2PValue >= 1 {
		return true
	}
	col0 := make([]bool, 0, len(cmd.chi2Cases))
	col1 := make([]bool, 0, len(cmd.chi2Cases))
	cases := make([]bool, 0, len(cmd.chi2Cases))
	for i, c := range cmd.chi2Cases {
		if colpair[0][i] < 0 {
			continue
		}
		col0 = append(col0, colpair[0][i] != 0)
		col1 = append(col1, colpair[1][i] != 0)
		cases = append(cases, c)
	}
	return len(cases) >= cmd.minCoverage &&
		(pvalue(cases, col0) <= cmd.chi2PValue || pvalue(cases, col1) <= cmd.chi2PValue)
}

func writeNumpyInt16(fnm string, out []int16, rows, cols int) error {
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
	log.WithFields(log.Fields{
		"filename": fnm,
		"rows":     rows,
		"cols":     cols,
	}).Infof("writing numpy: %s", fnm)
	npw.Shape = []int{rows, cols}
	npw.WriteInt16(out)
	err = bufw.Flush()
	if err != nil {
		return err
	}
	return output.Close()
}

func writeNumpyInt8(fnm string, out []int8, rows, cols int) error {
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
	log.WithFields(log.Fields{
		"filename": fnm,
		"rows":     rows,
		"cols":     cols,
	}).Infof("writing numpy: %s", fnm)
	npw.Shape = []int{rows, cols}
	npw.WriteInt8(out)
	err = bufw.Flush()
	if err != nil {
		return err
	}
	return output.Close()
}

func allele2homhet(colpair [2][]int8) {
	a, b := colpair[0], colpair[1]
	for i, av := range a {
		bv := b[i]
		if av < 0 || bv < 0 {
			// no-call
			a[i], b[i] = -1, -1
		} else if av > 0 && bv > 0 {
			// hom
			a[i], b[i] = 1, 0
		} else if av > 0 || bv > 0 {
			// het
			a[i], b[i] = 0, 1
		} else {
			// ref (or a different variant in same position)
			// (this is a no-op) a[i], b[i] = 0, 0
		}
	}
}
