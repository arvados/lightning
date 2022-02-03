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
	"unsafe"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
	"github.com/kshedden/gonpy"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type sliceNumpy struct {
	filter                filter
	threads               int
	chi2CaseControlColumn string
	chi2CaseControlFile   string
	chi2Cases             []bool
	chi2PValue            float64
	minCoverage           int
	cgnames               []string
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
	onehotSingle := flags.Bool("single-onehot", false, "generate one-hot tile-based matrix")
	onehotChunked := flags.Bool("chunked-onehot", false, "generate one-hot tile-based matrix per input chunk")
	flags.IntVar(&cmd.threads, "threads", 16, "number of memory-hungry assembly threads")
	flags.StringVar(&cmd.chi2CaseControlFile, "chi2-case-control-file", "", "tsv file or directory indicating cases and controls for Χ² test (if directory, all .tsv files will be read)")
	flags.StringVar(&cmd.chi2CaseControlColumn, "chi2-case-control-column", "", "name of case/control column in case-control files for Χ² test (value must be 0 for control, 1 for case)")
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

	if cmd.chi2PValue != 1 && (cmd.chi2CaseControlFile == "" || cmd.chi2CaseControlColumn == "") {
		log.Errorf("cannot use provided -chi2-p-value=%f because -chi2-case-control-file= or -chi2-case-control-column= value is empty", cmd.chi2PValue)
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
		err = runner.TranslatePaths(inputDir, regionsFilename, &cmd.chi2CaseControlFile)
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
			"-single-onehot=" + fmt.Sprintf("%v", *onehotSingle),
			"-chunked-onehot=" + fmt.Sprintf("%v", *onehotChunked),
			"-chi2-case-control-file=" + cmd.chi2CaseControlFile,
			"-chi2-case-control-column=" + cmd.chi2CaseControlColumn,
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

	infiles, err := allFiles(*inputDir, matchGobFile)
	if err != nil {
		return 1
	}
	if len(infiles) == 0 {
		err = fmt.Errorf("no input files found in %s", *inputDir)
		return 1
	}
	sort.Strings(infiles)

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

	cmd.cgnames = nil
	var tagset [][]byte
	DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
		if len(ent.TagSet) > 0 {
			tagset = ent.TagSet
		}
		for _, cseq := range ent.CompactSequences {
			if cseq.Name == *ref || *ref == "" {
				refseq = cseq.TileSequences
			}
		}
		for _, cg := range ent.CompactGenomes {
			if matchGenome.MatchString(cg.Name) {
				cmd.cgnames = append(cmd.cgnames, cg.Name)
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
	if len(tagset) == 0 {
		err = fmt.Errorf("tagset not found")
		return 1
	}

	taglib := &tagLibrary{}
	err = taglib.setTags(tagset)
	if err != nil {
		return 1
	}
	taglen := taglib.TagLen()

	if len(cmd.cgnames) == 0 {
		err = fmt.Errorf("no genomes found matching regexp %q", cmd.filter.MatchGenome)
		return 1
	}
	sort.Strings(cmd.cgnames)
	err = cmd.useCaseControlFiles()
	if err != nil {
		return 1
	}
	cmd.minCoverage = int(math.Ceil(cmd.filter.MinCoverage * float64(len(cmd.cgnames))))

	{
		labelsFilename := *outputDir + "/samples.csv"
		log.Infof("writing labels to %s", labelsFilename)
		var f *os.File
		f, err = os.Create(labelsFilename)
		if err != nil {
			return 1
		}
		defer f.Close()
		for i, name := range cmd.cgnames {
			cc := 0
			if cmd.chi2Cases != nil && cmd.chi2Cases[i] {
				cc = 1
			}
			_, err = fmt.Fprintf(f, "%d,%q,%d\n", i, trimFilenameForLabel(name), cc)
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
			foundthistag := false
			taglib.FindAll(tiledata[:len(tiledata)-1], func(tagid tagID, offset, _ int) {
				if !foundthistag && tagid == libref.Tag {
					foundthistag = true
					return
				}
				if dupref, ok := reftile[tagid]; ok {
					log.Printf("dropping reference tile %+v from %s @ %d, tag not unique, also found inside %+v from %s @ %d", tileLibRef{Tag: tagid, Variant: dupref.variant}, dupref.seqname, dupref.pos, libref, seqname, pos+offset+1)
					delete(reftile, tagid)
				} else {
					log.Printf("found tag %d at offset %d inside tile variant %+v on %s @ %d", tagid, offset, libref, seqname, pos+offset+1)
				}
				isdup[tagid] = true
			})
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
	var onehotIndirect [][2][]uint32 // [chunkIndex][axis][index]
	var onehotXrefs [][]onehotXref
	if *onehotSingle {
		onehotIndirect = make([][2][]uint32, len(infiles))
		onehotXrefs = make([][]onehotXref, len(infiles))
	}

	throttleMem := throttle{Max: cmd.threads} // TODO: estimate using mem and data size
	throttleNumpyMem := throttle{Max: cmd.threads/2 + 1}
	log.Info("generating annotations and numpy matrix for each slice")
	var done int64
	for infileIdx, infile := range infiles {
		infileIdx, infile := infileIdx, infile
		throttleMem.Go(func() error {
			seq := make(map[tagID][]TileVariant, 50000)
			cgs := make(map[string]CompactGenome, len(cmd.cgnames))
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
			tagstart := cgs[cmd.cgnames[0]].StartTag
			tagend := cgs[cmd.cgnames[0]].EndTag

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

			var onehotChunk [][]int8
			var onehotXref []onehotXref

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
				remap := variantRemap[tag-tagstart]
				maxv := tileVariantID(0)
				for _, v := range remap {
					if maxv < v {
						maxv = v
					}
				}
				if *onehotChunked || *onehotSingle {
					onehot, xrefs := cmd.tv2homhet(cgs, maxv, remap, tag, tagstart)
					onehotChunk = append(onehotChunk, onehot...)
					onehotXref = append(onehotXref, xrefs...)
				}
				fmt.Fprintf(annow, "%d,%d,%d,=,%s,%d,,,\n", tag, outcol, rt.variant, rt.seqname, rt.pos)
				variants := seq[tag]
				reftilestr := strings.ToUpper(string(rt.tiledata))

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
								make([]int8, len(cmd.cgnames)),
								make([]int8, len(cmd.cgnames)),
							}
						}
					}
					for row, name := range cmd.cgnames {
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

			if *onehotChunked {
				// transpose onehotChunk[col][row] to numpy[row*ncols+col]
				rows := len(cmd.cgnames)
				cols := len(onehotChunk)
				log.Infof("%04d: preparing onehot numpy (rows=%d, cols=%d, mem=%d)", infileIdx, len(cmd.cgnames), len(onehotChunk), len(cmd.cgnames)*len(onehotChunk))
				throttleNumpyMem.Acquire()
				out := onehotcols2int8(onehotChunk)
				fnm := fmt.Sprintf("%s/onehot.%04d.npy", *outputDir, infileIdx)
				err = writeNumpyInt8(fnm, out, rows, cols)
				if err != nil {
					return err
				}
				fnm = fmt.Sprintf("%s/onehot-columns.%04d.npy", *outputDir, infileIdx)
				err = writeNumpyInt32(fnm, onehotXref2int32(onehotXref), 4, len(onehotXref))
				if err != nil {
					return err
				}
				debug.FreeOSMemory()
				throttleNumpyMem.Release()
			}
			if *onehotSingle {
				onehotIndirect[infileIdx] = onehotChunk2Indirect(onehotChunk)
				onehotXrefs[infileIdx] = onehotXref
				n := len(onehotIndirect[infileIdx][0])
				log.Infof("%04d: keeping onehot coordinates in memory (n=%d, mem=%d)", infileIdx, n, n*8)
			}
			if !(*onehotSingle || *onehotChunked) || *mergeOutput || *hgvsSingle {
				log.Infof("%04d: preparing numpy", infileIdx)
				throttleNumpyMem.Acquire()
				rows := len(cmd.cgnames)
				cols := 2 * outcol
				out := make([]int16, rows*cols)
				for row, name := range cmd.cgnames {
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
				if !*mergeOutput && !*onehotChunked && !*onehotSingle {
					fnm := fmt.Sprintf("%s/matrix.%04d.npy", *outputDir, infileIdx)
					err = writeNumpyInt16(fnm, out, rows, cols)
					if err != nil {
						return err
					}
				}
			}
			debug.FreeOSMemory()
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
			rows := len(cmd.cgnames)
			cols := len(variants) * 2
			log.Infof("%s: building hgvs matrix (rows=%d, cols=%d, mem=%d)", seqname, rows, cols, rows*cols)
			out := make([]int8, rows*cols)
			for varIdx, variant := range variants {
				hgvsCols := hgvsCols[variant]
				for row := range cmd.cgnames {
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

		rows := len(cmd.cgnames)
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
					hgvsColPair = [2][]int16{make([]int16, len(cmd.cgnames)), make([]int16, len(cmd.cgnames))}
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
	if *onehotSingle {
		nzCount := 0
		for _, part := range onehotIndirect {
			nzCount += len(part[0])
		}
		onehot := make([]uint32, nzCount*2) // [r,r,r,...,c,c,c,...]
		var xrefs []onehotXref
		outcol := 0
		for i, part := range onehotIndirect {
			for i := range part[1] {
				part[1][i] += uint32(outcol)
			}
			copy(onehot[outcol:], part[0])
			copy(onehot[outcol+nzCount:], part[1])
			outcol += len(part[0])
			xrefs = append(xrefs, onehotXrefs[i]...)

			part[0] = nil
			part[1] = nil
			onehotXrefs[i] = nil
			debug.FreeOSMemory()
		}
		fnm := fmt.Sprintf("%s/onehot.npy", *outputDir)
		err = writeNumpyUint32(fnm, onehot, 2, nzCount)
		if err != nil {
			return 1
		}
		fnm = fmt.Sprintf("%s/onehot-columns.npy", *outputDir)
		err = writeNumpyInt32(fnm, onehotXref2int32(xrefs), 4, len(xrefs))
		if err != nil {
			return 1
		}
	}
	return 0
}

// Read case/control files, remove non-case/control entries from
// cmd.cgnames, and build cmd.chi2Cases.
func (cmd *sliceNumpy) useCaseControlFiles() error {
	if cmd.chi2CaseControlFile == "" {
		return nil
	}
	infiles, err := allFiles(cmd.chi2CaseControlFile, nil)
	if err != nil {
		return err
	}
	// index in cmd.cgnames => case(true) / control(false)
	cc := map[int]bool{}
	for _, infile := range infiles {
		f, err := open(infile)
		if err != nil {
			return err
		}
		buf, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		ccCol := -1
		for _, tsv := range bytes.Split(buf, []byte{'\n'}) {
			if len(tsv) == 0 {
				continue
			}
			split := strings.Split(string(tsv), "\t")
			if ccCol < 0 {
				// header row
				for col, name := range split {
					if name == cmd.chi2CaseControlColumn {
						ccCol = col
						break
					}
				}
				if ccCol < 0 {
					return fmt.Errorf("%s: no column named %q in header row %q", infile, cmd.chi2CaseControlColumn, tsv)
				}
				continue
			}
			if len(split) <= ccCol {
				continue
			}
			pattern := split[0]
			found := -1
			for i, name := range cmd.cgnames {
				if strings.Contains(name, pattern) {
					if found >= 0 {
						log.Warnf("pattern %q in %s matches multiple genome IDs (%qs, %q)", pattern, infile, cmd.cgnames[found], name)
					}
					found = i
				}
			}
			if found < 0 {
				log.Warnf("pattern %q in %s does not match any genome IDs", pattern, infile)
				continue
			}
			if split[ccCol] == "0" {
				cc[found] = false
			}
			if split[ccCol] == "1" {
				cc[found] = true
			}
		}
	}
	allnames := cmd.cgnames
	cmd.cgnames = nil
	cmd.chi2Cases = nil
	ncases := 0
	for i, name := range allnames {
		if cc, ok := cc[i]; ok {
			cmd.cgnames = append(cmd.cgnames, name)
			cmd.chi2Cases = append(cmd.chi2Cases, cc)
			if cc {
				ncases++
			}
		}
	}
	log.Printf("%d cases, %d controls, %d neither (dropped)", ncases, len(cmd.cgnames)-ncases, len(allnames)-len(cmd.cgnames))
	return nil
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
		(pvalue(col0, cases) <= cmd.chi2PValue || pvalue(col1, cases) <= cmd.chi2PValue)
}

func writeNumpyUint32(fnm string, out []uint32, rows, cols int) error {
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
		"bytes":    rows * cols * 4,
	}).Infof("writing numpy: %s", fnm)
	npw.Shape = []int{rows, cols}
	npw.WriteUint32(out)
	err = bufw.Flush()
	if err != nil {
		return err
	}
	return output.Close()
}

func writeNumpyInt32(fnm string, out []int32, rows, cols int) error {
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
		"bytes":    rows * cols * 4,
	}).Infof("writing numpy: %s", fnm)
	npw.Shape = []int{rows, cols}
	npw.WriteInt32(out)
	err = bufw.Flush()
	if err != nil {
		return err
	}
	return output.Close()
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
		"bytes":    rows * cols * 2,
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
		"bytes":    rows * cols,
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

type onehotXref struct {
	tag     tagID
	variant tileVariantID
	het     bool
	pvalue  float64
}

const onehotXrefSize = unsafe.Sizeof(onehotXref{})

// Build onehot matrix (m[variant*2+isHet][genome] == 0 or 1) for all
// variants of a single tile/tag#.
//
// Return nil if no tile variant passes Χ² filter.
func (cmd *sliceNumpy) tv2homhet(cgs map[string]CompactGenome, maxv tileVariantID, remap []tileVariantID, tag, chunkstarttag tagID) ([][]int8, []onehotXref) {
	if maxv < 2 {
		// everyone has the most common variant
		return nil, nil
	}
	tagoffset := tag - chunkstarttag
	coverage := 0
	for _, cg := range cgs {
		if cg.Variants[tagoffset*2] > 0 && cg.Variants[tagoffset*2+1] > 0 {
			coverage++
		}
	}
	if coverage < cmd.minCoverage {
		return nil, nil
	}
	obs := make([][]bool, (maxv+1)*2) // 2 slices (hom + het) for each variant#
	for i := range obs {
		obs[i] = make([]bool, len(cmd.cgnames))
	}
	for cgid, name := range cmd.cgnames {
		cgvars := cgs[name].Variants
		for v := tileVariantID(2); v <= maxv; v++ {
			if remap[cgvars[tagoffset*2]] == v && remap[cgvars[tagoffset*2+1]] == v {
				obs[v*2][cgid] = true
			} else if remap[cgvars[tagoffset*2]] == v || remap[cgvars[tagoffset*2+1]] == v {
				obs[v*2+1][cgid] = true
			}
		}
	}
	var onehot [][]int8
	var xref []onehotXref
	for homcol := 4; homcol < len(obs); homcol += 2 {
		p := [2]float64{
			pvalue(obs[homcol], cmd.chi2Cases),
			pvalue(obs[homcol+1], cmd.chi2Cases),
		}
		if cmd.chi2PValue < 1 && !(p[0] < cmd.chi2PValue || p[1] < cmd.chi2PValue) {
			continue
		}
		for het := 0; het < 2; het++ {
			onehot = append(onehot, bool2int8(obs[homcol+het]))
			xref = append(xref, onehotXref{
				tag:     tag,
				variant: tileVariantID(homcol / 2),
				het:     het == 1,
				pvalue:  p[het],
			})
		}
	}
	return onehot, xref
}

func bool2int8(in []bool) []int8 {
	out := make([]int8, len(in))
	for i, v := range in {
		if v {
			out[i] = 1
		}
	}
	return out
}

// convert a []onehotXref with length N to a numpy-style []int32
// matrix with N columns, one row per field of onehotXref struct.
//
// Hom/het row contains hom=0, het=1.
//
// P-value row contains 1000000x actual p-value.
func onehotXref2int32(xrefs []onehotXref) []int32 {
	xcols := len(xrefs)
	xdata := make([]int32, 4*xcols)
	for i, xref := range xrefs {
		xdata[i] = int32(xref.tag)
		xdata[xcols+i] = int32(xref.variant)
		if xref.het {
			xdata[xcols*2+i] = 1
		}
		xdata[xcols*3+i] = int32(xref.pvalue * 1000000)
	}
	return xdata
}

// transpose onehot data from in[col][row] to numpy-style
// out[row*cols+col].
func onehotcols2int8(in [][]int8) []int8 {
	if len(in) == 0 {
		return nil
	}
	cols := len(in)
	rows := len(in[0])
	out := make([]int8, rows*cols)
	for row := 0; row < rows; row++ {
		outrow := out[row*cols:]
		for col, incol := range in {
			outrow[col] = incol[row]
		}
	}
	return out
}

// Return [2][]uint32{rowIndices, colIndices} indicating which
// elements of matrixT[c][r] have non-zero values.
func onehotChunk2Indirect(matrixT [][]int8) [2][]uint32 {
	var nz [2][]uint32
	for c, col := range matrixT {
		for r, val := range col {
			if val != 0 {
				nz[0] = append(nz[0], uint32(r))
				nz[1] = append(nz[1], uint32(c))
			}
		}
	}
	return nz
}
