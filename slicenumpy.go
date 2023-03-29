// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
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
	"github.com/james-bowman/nlp"
	"github.com/kshedden/gonpy"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
	"gonum.org/v1/gonum/mat"
)

const annotationMaxTileSpan = 100

type sliceNumpy struct {
	filter             filter
	threads            int
	chi2Cases          []bool
	chi2PValue         float64
	pvalueMinFrequency float64
	pcaComponents      int
	minCoverage        int
	includeVariant1    bool
	debugTag           tagID

	cgnames         []string
	samples         []sampleInfo
	trainingSet     []int // samples index => training set index, or -1 if not in training set
	trainingSetSize int
	pvalue          func(onehot []bool) float64
	pvalueCallCount int64
}

func (cmd *sliceNumpy) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := cmd.run(prog, args, stdin, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return 1
	}
	return 0
}

func (cmd *sliceNumpy) run(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	arvadosRAM := flags.Int("arvados-ram", 750000000000, "amount of memory to request for arvados container (`bytes`)")
	arvadosVCPUs := flags.Int("arvados-vcpus", 96, "number of VCPUs to request for arvados container")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	preemptible := flags.Bool("preemptible", true, "request preemptible instance")
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
	samplesFilename := flags.String("samples", "", "`samples.csv` file with training/validation and case/control groups (see 'lightning choose-samples')")
	caseControlOnly := flags.Bool("case-control-only", false, "drop samples that are not in case/control groups")
	onlyPCA := flags.Bool("pca", false, "run principal component analysis, write components to pca.npy and samples.csv")
	flags.IntVar(&cmd.pcaComponents, "pca-components", 4, "number of PCA components to compute / use in logistic regression")
	maxPCATiles := flags.Int("max-pca-tiles", 0, "maximum tiles to use as PCA input (filter, then drop every 2nd colum pair until below max)")
	debugTag := flags.Int("debug-tag", -1, "log debugging details about specified tag")
	flags.IntVar(&cmd.threads, "threads", 16, "number of memory-hungry assembly threads, and number of VCPUs to request for arvados container")
	flags.Float64Var(&cmd.chi2PValue, "chi2-p-value", 1, "do Χ² test (or logistic regression if -samples file has PCA components) and omit columns with p-value above this threshold")
	flags.Float64Var(&cmd.pvalueMinFrequency, "pvalue-min-frequency", 0.01, "skip p-value calculation on tile variants below this frequency in the training set")
	flags.BoolVar(&cmd.includeVariant1, "include-variant-1", false, "include most common variant when building one-hot matrix")
	cmd.filter.Flags(flags)
	err := flags.Parse(args)
	if err == flag.ErrHelp {
		return nil
	} else if err != nil {
		return err
	} else if flags.NArg() > 0 {
		return fmt.Errorf("errant command line arguments after parsed flags: %v", flags.Args())
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if cmd.chi2PValue != 1 && *samplesFilename == "" {
		return fmt.Errorf("cannot use provided -chi2-p-value=%f because -samples= value is empty", cmd.chi2PValue)
	}

	cmd.debugTag = tagID(*debugTag)

	if !*runlocal {
		runner := arvadosContainerRunner{
			Name:        "lightning slice-numpy",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         int64(*arvadosRAM),
			VCPUs:       *arvadosVCPUs,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
			Preemptible: *preemptible,
		}
		err = runner.TranslatePaths(inputDir, regionsFilename, samplesFilename)
		if err != nil {
			return err
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
			"-samples=" + *samplesFilename,
			"-case-control-only=" + fmt.Sprintf("%v", *caseControlOnly),
			"-pca=" + fmt.Sprintf("%v", *onlyPCA),
			"-pca-components=" + fmt.Sprintf("%d", cmd.pcaComponents),
			"-max-pca-tiles=" + fmt.Sprintf("%d", *maxPCATiles),
			"-chi2-p-value=" + fmt.Sprintf("%f", cmd.chi2PValue),
			"-pvalue-min-frequency=" + fmt.Sprintf("%f", cmd.pvalueMinFrequency),
			"-include-variant-1=" + fmt.Sprintf("%v", cmd.includeVariant1),
			"-debug-tag=" + fmt.Sprintf("%d", cmd.debugTag),
		}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, output)
		return nil
	}

	infiles, err := allFiles(*inputDir, matchGobFile)
	if err != nil {
		return err
	}
	if len(infiles) == 0 {
		err = fmt.Errorf("no input files found in %s", *inputDir)
		return err
	}
	sort.Strings(infiles)

	var refseq map[string][]tileLibRef
	var reftiledata = make(map[tileLibRef][]byte, 11000000)
	in0, err := open(infiles[0])
	if err != nil {
		return err
	}

	matchGenome, err := regexp.Compile(cmd.filter.MatchGenome)
	if err != nil {
		err = fmt.Errorf("-match-genome: invalid regexp: %q", cmd.filter.MatchGenome)
		return err
	}

	if *samplesFilename != "" {
		cmd.samples, err = loadSampleInfo(*samplesFilename)
		if err != nil {
			return err
		}
	} else if *caseControlOnly {
		return fmt.Errorf("-case-control-only does not make sense without -samples")
	}

	cmd.cgnames = nil
	var tagset [][]byte
	err = DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
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
		return err
	}
	in0.Close()
	if refseq == nil {
		err = fmt.Errorf("%s: reference sequence not found", infiles[0])
		return err
	}
	if len(tagset) == 0 {
		err = fmt.Errorf("tagset not found")
		return err
	}

	taglib := &tagLibrary{}
	err = taglib.setTags(tagset)
	if err != nil {
		return err
	}
	taglen := taglib.TagLen()
	sort.Strings(cmd.cgnames)

	if len(cmd.cgnames) == 0 {
		return fmt.Errorf("fatal: 0 matching samples in library, nothing to do")
	}
	cmd.trainingSet = make([]int, len(cmd.cgnames))
	if *samplesFilename == "" {
		cmd.trainingSetSize = len(cmd.cgnames)
		for i, name := range cmd.cgnames {
			cmd.samples = append(cmd.samples, sampleInfo{
				id:         trimFilenameForLabel(name),
				isTraining: true,
			})
			cmd.trainingSet[i] = i
		}
	} else if len(cmd.cgnames) != len(cmd.samples) {
		return fmt.Errorf("mismatched sample list: %d samples in library, %d in %s", len(cmd.cgnames), len(cmd.samples), *samplesFilename)
	} else {
		for i, name := range cmd.cgnames {
			if s := trimFilenameForLabel(name); s != cmd.samples[i].id {
				return fmt.Errorf("mismatched sample list: sample %d is %q in library, %q in %s", i, s, cmd.samples[i].id, *samplesFilename)
			}
		}
		if *caseControlOnly {
			for i := 0; i < len(cmd.samples); i++ {
				if !cmd.samples[i].isTraining && !cmd.samples[i].isValidation {
					if i+1 < len(cmd.samples) {
						copy(cmd.samples[i:], cmd.samples[i+1:])
						copy(cmd.cgnames[i:], cmd.cgnames[i+1:])
					}
					cmd.samples = cmd.samples[:len(cmd.samples)-1]
					cmd.cgnames = cmd.cgnames[:len(cmd.cgnames)-1]
					i--
				}
			}
		}
		cmd.chi2Cases = nil
		cmd.trainingSetSize = 0
		for i := range cmd.cgnames {
			if cmd.samples[i].isTraining {
				cmd.trainingSet[i] = cmd.trainingSetSize
				cmd.trainingSetSize++
				cmd.chi2Cases = append(cmd.chi2Cases, cmd.samples[i].isCase)
			} else {
				cmd.trainingSet[i] = -1
			}
		}
		if cmd.pvalue == nil {
			cmd.pvalue = func(onehot []bool) float64 {
				return pvalue(onehot, cmd.chi2Cases)
			}
		}
	}
	if cmd.filter.MinCoverage == 1 {
		// In the generic formula below, floating point
		// arithmetic can effectively push the coverage
		// threshold above 1.0, which is impossible/useless.
		// 1.0 needs to mean exactly 100% coverage.
		cmd.minCoverage = len(cmd.cgnames)
	} else {
		cmd.minCoverage = int(math.Ceil(cmd.filter.MinCoverage * float64(len(cmd.cgnames))))
	}

	if len(cmd.samples[0].pcaComponents) > 0 {
		cmd.pvalue = glmPvalueFunc(cmd.samples, cmd.pcaComponents)
		// Unfortunately, statsmodel/glm lib logs stuff to
		// os.Stdout when it panics on an unsolvable
		// problem. We recover() from the panic in glm.go, but
		// we also need to commandeer os.Stdout to avoid
		// producing large quantities of logs.
		stdoutWas := os.Stdout
		defer func() { os.Stdout = stdoutWas }()
		os.Stdout, err = os.Open(os.DevNull)
		if err != nil {
			return err
		}
	}

	// cgnamemap[name]==true for samples that we are including in
	// output
	cgnamemap := map[string]bool{}
	for _, name := range cmd.cgnames {
		cgnamemap[name] = true
	}

	err = writeSampleInfo(cmd.samples, *outputDir)
	if err != nil {
		return err
	}

	log.Info("indexing reference tiles")
	type reftileinfo struct {
		variant  tileVariantID
		seqname  string // chr1
		pos      int    // distance from start of chromosome to starttag
		tiledata []byte // acgtggcaa...
		excluded bool   // true if excluded by regions file
		nexttag  tagID  // tagID of following tile (-1 for last tag of chromosome)
	}
	isdup := map[tagID]bool{}
	reftile := map[tagID]*reftileinfo{}
	for seqname, cseq := range refseq {
		pos := 0
		lastreftag := tagID(-1)
		for _, libref := range cseq {
			if cmd.filter.MaxTag >= 0 && libref.Tag > tagID(cmd.filter.MaxTag) {
				continue
			}
			tiledata := reftiledata[libref]
			if len(tiledata) == 0 {
				err = fmt.Errorf("missing tiledata for tag %d variant %d in %s in ref", libref.Tag, libref.Variant, seqname)
				return err
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
					nexttag:  -1,
				}
				if lastreftag >= 0 {
					reftile[lastreftag].nexttag = libref.Tag
				}
				lastreftag = libref.Tag
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
			return err
		}
		log.Printf("before applying mask, len(reftile) == %d", len(reftile))
		log.Printf("deleting reftile entries for regions outside %d intervals", mask.Len())
		for _, rt := range reftile {
			if !mask.Check(strings.TrimPrefix(rt.seqname, "chr"), rt.pos, rt.pos+len(rt.tiledata)) {
				rt.excluded = true
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
				return err
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
	var onehotChunkSize []uint32
	var onehotXrefs [][]onehotXref
	if *onehotSingle || *onlyPCA {
		onehotIndirect = make([][2][]uint32, len(infiles))
		onehotChunkSize = make([]uint32, len(infiles))
		onehotXrefs = make([][]onehotXref, len(infiles))
	}
	chunkStartTag := make([]tagID, len(infiles))

	throttleMem := throttle{Max: cmd.threads} // TODO: estimate using mem and data size
	throttleNumpyMem := throttle{Max: cmd.threads/2 + 1}
	log.Info("generating annotations and numpy matrix for each slice")
	var errSkip = errors.New("skip infile")
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
					// Skip tile with no
					// corresponding ref tile, if
					// mask is in play (we can't
					// determine coordinates for
					// these)
					if mask != nil && reftile[tv.Tag] == nil {
						continue
					}
					// Skip tile whose
					// corresponding ref tile is
					// outside target regions --
					// unless it's a potential
					// spanning tile.
					if mask != nil && reftile[tv.Tag].excluded &&
						(int(tv.Tag+1) >= len(tagset) ||
							(bytes.HasSuffix(tv.Sequence, tagset[tv.Tag+1]) && reftile[tv.Tag+1] != nil && !reftile[tv.Tag+1].excluded)) {
						continue
					}
					if tv.Tag == cmd.debugTag {
						log.Printf("infile %d %s tag %d variant %d hash %x", infileIdx, infile, tv.Tag, tv.Variant, tv.Blake2b[:3])
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
					if cmd.filter.MaxTag >= 0 && cg.StartTag > tagID(cmd.filter.MaxTag) {
						return errSkip
					}
					if !cgnamemap[cg.Name] {
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
			if err == errSkip {
				return nil
			} else if err != nil {
				return fmt.Errorf("%04d: DecodeLibrary(%s): err", infileIdx, infile)
			}
			tagstart := cgs[cmd.cgnames[0]].StartTag
			tagend := cgs[cmd.cgnames[0]].EndTag
			chunkStartTag[infileIdx] = tagstart

			// TODO: filters

			log.Infof("%04d: renumber/dedup variants for tags %d-%d", infileIdx, tagstart, tagend)
			variantRemap := make([][]tileVariantID, tagend-tagstart)
			throttleCPU := throttle{Max: runtime.GOMAXPROCS(0)}
			for tag, variants := range seq {
				tag, variants := tag, variants
				throttleCPU.Go(func() error {
					alleleCoverage := 0
					count := make(map[[blake2b.Size256]byte]int, len(variants))

					rt := reftile[tag]
					if rt != nil {
						count[blake2b.Sum256(rt.tiledata)] = 0
					}

					for cgname, cg := range cgs {
						idx := int(tag-tagstart) * 2
						for allele := 0; allele < 2; allele++ {
							v := cg.Variants[idx+allele]
							if v > 0 && len(variants[v].Sequence) > 0 {
								count[variants[v].Blake2b]++
								alleleCoverage++
							}
							if v > 0 && tag == cmd.debugTag {
								log.Printf("tag %d cg %s allele %d tv %d hash %x count is now %d", tag, cgname, allele, v, variants[v].Blake2b[:3], count[variants[v].Blake2b])
							}
						}
					}
					if alleleCoverage < cmd.minCoverage*2 {
						idx := int(tag-tagstart) * 2
						for _, cg := range cgs {
							cg.Variants[idx] = 0
							cg.Variants[idx+1] = 0
						}
						if tag == cmd.debugTag {
							log.Printf("tag %d alleleCoverage %d < min %d, sample data wiped", tag, alleleCoverage, cmd.minCoverage*2)
						}
						return nil
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
					if tag == cmd.debugTag {
						for h, r := range rank {
							log.Printf("tag %d rank(%x) = %v", tag, h[:3], r)
						}
					}
					// remap[v] will be the new
					// variant number for original
					// variant number v.
					remap := make([]tileVariantID, len(variants))
					for i, tv := range variants {
						remap[i] = rank[tv.Blake2b]
					}
					if tag == cmd.debugTag {
						for in, out := range remap {
							if out > 0 {
								log.Printf("tag %d remap %d => %d", tag, in, out)
							}
						}
					}
					variantRemap[tag-tagstart] = remap
					if rt != nil {
						refrank := rank[blake2b.Sum256(rt.tiledata)]
						if tag == cmd.debugTag {
							log.Printf("tag %d reftile variant %d => %d", tag, rt.variant, refrank)
						}
						rt.variant = refrank
					}
					return nil
				})
			}
			throttleCPU.Wait()

			var onehotChunk [][]int8
			var onehotXref []onehotXref

			var annotationsFilename string
			if *onlyPCA {
				annotationsFilename = "/dev/null"
			} else {
				annotationsFilename = fmt.Sprintf("%s/matrix.%04d.annotations.csv", *outputDir, infileIdx)
				log.Infof("%04d: writing %s", infileIdx, annotationsFilename)
			}
			annof, err := os.Create(annotationsFilename)
			if err != nil {
				return err
			}
			annow := bufio.NewWriterSize(annof, 1<<20)
			outcol := 0
			for tag := tagstart; tag < tagend; tag++ {
				rt := reftile[tag]
				if rt == nil && mask != nil {
					// With no ref tile, we don't
					// have coordinates to say
					// this is in the desired
					// regions -- so it's not.
					// TODO: handle ref spanning
					// tile case.
					continue
				}
				if rt != nil && rt.excluded {
					// TODO: don't skip yet --
					// first check for spanning
					// tile variants that
					// intersect non-excluded ref
					// tiles.
					continue
				}
				if cmd.filter.MaxTag >= 0 && tag > tagID(cmd.filter.MaxTag) {
					break
				}
				remap := variantRemap[tag-tagstart]
				if remap == nil {
					// was not assigned above,
					// because minCoverage
					outcol++
					continue
				}
				maxv := tileVariantID(0)
				for _, v := range remap {
					if maxv < v {
						maxv = v
					}
				}
				if *onehotChunked || *onehotSingle || *onlyPCA {
					onehot, xrefs := cmd.tv2homhet(cgs, maxv, remap, tag, tagstart, seq)
					if tag == cmd.debugTag {
						log.WithFields(logrus.Fields{
							"onehot": onehot,
							"xrefs":  xrefs,
						}).Info("tv2homhet()")
					}
					onehotChunk = append(onehotChunk, onehot...)
					onehotXref = append(onehotXref, xrefs...)
				}
				if *onlyPCA {
					outcol++
					continue
				}
				if rt == nil {
					// Reference does not use any
					// variant of this tile
					//
					// TODO: diff against the
					// relevant portion of the
					// ref's spanning tile
					outcol++
					continue
				}
				fmt.Fprintf(annow, "%d,%d,%d,=,%s,%d,,,\n", tag, outcol, rt.variant, rt.seqname, rt.pos)
				variants := seq[tag]
				reftilestr := strings.ToUpper(string(rt.tiledata))

				done := make([]bool, maxv+1)
				variantDiffs := make([][]hgvs.Variant, maxv+1)
				for v, tv := range variants {
					v := remap[v]
					if v == 0 || v == rt.variant || done[v] {
						continue
					} else {
						done[v] = true
					}
					if len(tv.Sequence) < taglen {
						continue
					}
					// if reftilestr doesn't end
					// in the same tag as tv,
					// extend reftilestr with
					// following ref tiles until
					// it does (up to an arbitrary
					// sanity-check limit)
					reftilestr := reftilestr
					endtagstr := strings.ToUpper(string(tv.Sequence[len(tv.Sequence)-taglen:]))
					for i, rt := 0, rt; i < annotationMaxTileSpan && !strings.HasSuffix(reftilestr, endtagstr) && rt.nexttag >= 0; i++ {
						rt = reftile[rt.nexttag]
						if rt == nil {
							break
						}
						reftilestr += strings.ToUpper(string(rt.tiledata[taglen:]))
					}
					if mask != nil && !mask.Check(strings.TrimPrefix(rt.seqname, "chr"), rt.pos, rt.pos+len(reftilestr)) {
						continue
					}
					if !strings.HasSuffix(reftilestr, endtagstr) {
						fmt.Fprintf(annow, "%d,%d,%d,,%s,%d,,,\n", tag, outcol, v, rt.seqname, rt.pos)
						continue
					}
					if lendiff := len(reftilestr) - len(tv.Sequence); lendiff < -1000 || lendiff > 1000 {
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
				log.Infof("%04d: preparing onehot numpy (rows=%d, cols=%d, mem=%d)", infileIdx, rows, cols, rows*cols)
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
			if *onehotSingle || *onlyPCA {
				onehotIndirect[infileIdx] = onehotChunk2Indirect(onehotChunk)
				onehotChunkSize[infileIdx] = uint32(len(onehotChunk))
				onehotXrefs[infileIdx] = onehotXref
				n := len(onehotIndirect[infileIdx][0])
				log.Infof("%04d: keeping onehot coordinates in memory (n=%d, mem=%d)", infileIdx, n, n*8*2)
			}
			if !(*onehotSingle || *onehotChunked || *onlyPCA) || *mergeOutput || *hgvsSingle {
				log.Infof("%04d: preparing numpy (rows=%d, cols=%d)", infileIdx, len(cmd.cgnames), 2*outcol)
				throttleNumpyMem.Acquire()
				rows := len(cmd.cgnames)
				cols := 2 * outcol
				out := make([]int16, rows*cols)
				for row, name := range cmd.cgnames {
					outidx := row * cols
					for col, v := range cgs[name].Variants {
						tag := tagstart + tagID(col/2)
						if cmd.filter.MaxTag >= 0 && tag > tagID(cmd.filter.MaxTag) {
							break
						}
						if rt := reftile[tag]; rt == nil || rt.excluded {
							continue
						}
						if v == 0 {
							out[outidx] = 0 // tag not found / spanning tile
						} else if variants, ok := seq[tag]; ok && int(v) < len(variants) && len(variants[v].Sequence) > 0 {
							out[outidx] = int16(variantRemap[tag-tagstart][v])
						} else {
							out[outidx] = -1 // low quality tile variant
						}
						if tag == cmd.debugTag {
							log.Printf("tag %d row %d col %d outidx %d v %d out %d", tag, row, col, outidx, v, out[outidx])
						}
						outidx++
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
		return err
	}

	if *hgvsChunked {
		log.Info("flushing hgvsCols temp files")
		for seqname := range refseq {
			close(encodeHGVSTodo[seqname])
		}
		err = encodeHGVS.Wait()
		if err != nil {
			return err
		}
		for seqname := range refseq {
			log.Infof("%s: reading hgvsCols from temp file", seqname)
			f := tmpHGVSCols[seqname]
			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}
			var hgvsCols hgvsColSet
			dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<24))
			for err == nil {
				err = dec.Decode(&hgvsCols)
			}
			if err != io.EOF {
				return err
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
				return err
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
				return err
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
				return err
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
				return err
			}
			if *mergeOutput {
				err = os.Remove(annotationsFilename)
				if err != nil {
					return err
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
						return err
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
				return err
			}
			err = annof.Close()
			if err != nil {
				return err
			}
			err = writeNumpyInt16(fmt.Sprintf("%s/matrix.npy", *outputDir), out, rows, cols)
			if err != nil {
				return err
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
				return err
			}

			fnm := fmt.Sprintf("%s/hgvs.annotations.csv", *outputDir)
			log.Printf("writing hgvs labels: %s", fnm)
			err = ioutil.WriteFile(fnm, hgvsLabels.Bytes(), 0777)
			if err != nil {
				return err
			}
		}
	}
	if *onehotSingle || *onlyPCA {
		nzCount := 0
		for _, part := range onehotIndirect {
			nzCount += len(part[0])
		}
		onehot := make([]uint32, nzCount*2) // [r,r,r,...,c,c,c,...]
		var xrefs []onehotXref
		chunkOffset := uint32(0)
		outcol := 0
		for i, part := range onehotIndirect {
			for i := range part[1] {
				part[1][i] += chunkOffset
			}
			copy(onehot[outcol:], part[0])
			copy(onehot[outcol+nzCount:], part[1])
			xrefs = append(xrefs, onehotXrefs[i]...)

			outcol += len(part[0])
			chunkOffset += onehotChunkSize[i]

			part[0] = nil
			part[1] = nil
			onehotXrefs[i] = nil
			debug.FreeOSMemory()
		}
		if *onehotSingle {
			fnm := fmt.Sprintf("%s/onehot.npy", *outputDir)
			err = writeNumpyUint32(fnm, onehot, 2, nzCount)
			if err != nil {
				return err
			}
			fnm = fmt.Sprintf("%s/onehot-columns.npy", *outputDir)
			err = writeNumpyInt32(fnm, onehotXref2int32(xrefs), 5, len(xrefs))
			if err != nil {
				return err
			}
			fnm = fmt.Sprintf("%s/stats.json", *outputDir)
			j, err := json.Marshal(map[string]interface{}{
				"pvalueCallCount": cmd.pvalueCallCount,
			})
			if err != nil {
				return err
			}
			err = os.WriteFile(fnm, j, 0777)
			if err != nil {
				return err
			}
		}
		if *onlyPCA {
			cols := 0
			for _, c := range onehot[nzCount:] {
				if int(c) >= cols {
					cols = int(c) + 1
				}
			}
			if cols == 0 {
				return fmt.Errorf("cannot do PCA: one-hot matrix is empty")
			}
			log.Printf("have %d one-hot cols", cols)
			stride := 1
			for *maxPCATiles > 0 && cols > *maxPCATiles*2 {
				cols = (cols + 1) / 2
				stride = stride * 2
			}
			if cols%2 == 1 {
				// we work with pairs of columns
				cols++
			}
			log.Printf("creating full matrix (%d rows) and training matrix (%d rows) with %d cols, stride %d", len(cmd.cgnames), cmd.trainingSetSize, cols, stride)
			mtxFull := mat.NewDense(len(cmd.cgnames), cols, nil)
			mtxTrain := mat.NewDense(cmd.trainingSetSize, cols, nil)
			for i, c := range onehot[nzCount:] {
				if int(c/2)%stride == 0 {
					outcol := int(c/2)/stride*2 + int(c)%2
					mtxFull.Set(int(onehot[i]), outcol, 1)
					if trainRow := cmd.trainingSet[int(onehot[i])]; trainRow >= 0 {
						mtxTrain.Set(trainRow, outcol, 1)
					}
				}
			}
			log.Print("fitting")
			transformer := nlp.NewPCA(cmd.pcaComponents)
			transformer.Fit(mtxTrain.T())
			log.Printf("transforming")
			pca, err := transformer.Transform(mtxFull.T())
			if err != nil {
				return err
			}
			pca = pca.T()
			outrows, outcols := pca.Dims()
			log.Printf("copying result to numpy output array: %d rows, %d cols", outrows, outcols)
			out := make([]float64, outrows*outcols)
			for i := 0; i < outrows; i++ {
				for j := 0; j < outcols; j++ {
					out[i*outcols+j] = pca.At(i, j)
				}
			}
			fnm := fmt.Sprintf("%s/pca.npy", *outputDir)
			log.Printf("writing numpy: %s", fnm)
			output, err := os.OpenFile(fnm, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
			if err != nil {
				return err
			}
			npw, err := gonpy.NewWriter(nopCloser{output})
			if err != nil {
				return fmt.Errorf("gonpy.NewWriter: %w", err)
			}
			npw.Shape = []int{outrows, outcols}
			err = npw.WriteFloat64(out)
			if err != nil {
				return fmt.Errorf("WriteFloat64: %w", err)
			}
			err = output.Close()
			if err != nil {
				return err
			}
			log.Print("done")

			log.Print("copying pca components to sampleInfo")
			for i := range cmd.samples {
				cmd.samples[i].pcaComponents = make([]float64, outcols)
				for c := 0; c < outcols; c++ {
					cmd.samples[i].pcaComponents[i] = pca.At(i, c)
				}
			}
			log.Print("done")

			err = writeSampleInfo(cmd.samples, *outputDir)
			if err != nil {
				return err
			}
		}
	}
	if !*mergeOutput && !*onehotChunked && !*onehotSingle && !*onlyPCA {
		tagoffsetFilename := *outputDir + "/chunk-tag-offset.csv"
		log.Infof("writing tag offsets to %s", tagoffsetFilename)
		var f *os.File
		f, err = os.Create(tagoffsetFilename)
		if err != nil {
			return err
		}
		defer f.Close()
		for idx, offset := range chunkStartTag {
			_, err = fmt.Fprintf(f, "%q,%d\n", fmt.Sprintf("matrix.%04d.npy", idx), offset)
			if err != nil {
				err = fmt.Errorf("write %s: %w", tagoffsetFilename, err)
				return err
			}
		}
		err = f.Close()
		if err != nil {
			err = fmt.Errorf("close %s: %w", tagoffsetFilename, err)
			return err
		}
	}

	return nil
}

type sampleInfo struct {
	id            string
	isCase        bool
	isControl     bool
	isTraining    bool
	isValidation  bool
	pcaComponents []float64
}

// Read samples.csv file with case/control and training/validation
// flags.
func loadSampleInfo(samplesFilename string) ([]sampleInfo, error) {
	var si []sampleInfo
	f, err := open(samplesFilename)
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	lineNum := 0
	for _, csv := range bytes.Split(buf, []byte{'\n'}) {
		lineNum++
		if len(csv) == 0 {
			continue
		}
		split := strings.Split(string(csv), ",")
		if len(split) < 4 {
			return nil, fmt.Errorf("%d fields < 4 in %s line %d: %q", len(split), samplesFilename, lineNum, csv)
		}
		if split[0] == "Index" && split[1] == "SampleID" && split[2] == "CaseControl" && split[3] == "TrainingValidation" {
			continue
		}
		idx, err := strconv.Atoi(split[0])
		if err != nil {
			if lineNum == 1 {
				return nil, fmt.Errorf("header does not look right: %q", csv)
			}
			return nil, fmt.Errorf("%s line %d: index: %s", samplesFilename, lineNum, err)
		}
		if idx != len(si) {
			return nil, fmt.Errorf("%s line %d: index %d out of order", samplesFilename, lineNum, idx)
		}
		var pcaComponents []float64
		if len(split) > 4 {
			for _, s := range split[4:] {
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("%s line %d: cannot parse float %q: %s", samplesFilename, lineNum, s, err)
				}
				pcaComponents = append(pcaComponents, f)
			}
		}
		si = append(si, sampleInfo{
			id:            split[1],
			isCase:        split[2] == "1",
			isControl:     split[2] == "0",
			isTraining:    split[3] == "1",
			isValidation:  split[3] == "0" && len(split[2]) > 0, // fix errant 0s in input
			pcaComponents: pcaComponents,
		})
	}
	return si, nil
}

func writeSampleInfo(samples []sampleInfo, outputDir string) error {
	fnm := outputDir + "/samples.csv"
	log.Infof("writing sample metadata to %s", fnm)
	f, err := os.Create(fnm)
	if err != nil {
		return err
	}
	defer f.Close()
	pcaLabels := ""
	if len(samples) > 0 {
		for i := range samples[0].pcaComponents {
			pcaLabels += fmt.Sprintf(",PCA%d", i)
		}
	}
	_, err = fmt.Fprintf(f, "Index,SampleID,CaseControl,TrainingValidation%s\n", pcaLabels)
	if err != nil {
		return err
	}
	for i, si := range samples {
		var cc, tv string
		if si.isCase {
			cc = "1"
		} else if si.isControl {
			cc = "0"
		}
		if si.isTraining {
			tv = "1"
		} else if si.isValidation {
			tv = "0"
		}
		var pcavals string
		for _, pcaval := range si.pcaComponents {
			pcavals += fmt.Sprintf(",%f", pcaval)
		}
		_, err = fmt.Fprintf(f, "%d,%s,%s,%s%s\n", i, si.id, cc, tv, pcavals)
		if err != nil {
			return fmt.Errorf("write %s: %w", fnm, err)
		}
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("close %s: %w", fnm, err)
	}
	log.Print("done")
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
	hom     bool
	pvalue  float64
}

const onehotXrefSize = unsafe.Sizeof(onehotXref{})

// Build onehot matrix (m[tileVariantIndex][genome] == 0 or 1) for all
// variants of a single tile/tag#.
//
// Return nil if no tile variant passes Χ² filter.
func (cmd *sliceNumpy) tv2homhet(cgs map[string]CompactGenome, maxv tileVariantID, remap []tileVariantID, tag, chunkstarttag tagID, seq map[tagID][]TileVariant) ([][]int8, []onehotXref) {
	if tag == cmd.debugTag {
		tv := make([]tileVariantID, len(cmd.cgnames)*2)
		for i, name := range cmd.cgnames {
			copy(tv[i*2:(i+1)*2], cgs[name].Variants[(tag-chunkstarttag)*2:])
		}
		log.WithFields(logrus.Fields{
			"cgs[i].Variants[tag*2+j]": tv,
			"maxv":                     maxv,
			"remap":                    remap,
			"tag":                      tag,
			"chunkstarttag":            chunkstarttag,
		}).Info("tv2homhet()")
	}
	if maxv < 1 || (maxv < 2 && !cmd.includeVariant1) {
		// everyone has the most common variant (of the variants we don't drop)
		return nil, nil
	}
	tagoffset := tag - chunkstarttag
	coverage := 0
	for _, cg := range cgs {
		alleles := 0
		for _, v := range cg.Variants[tagoffset*2 : tagoffset*2+2] {
			if v > 0 && int(v) < len(seq[tag]) && len(seq[tag][v].Sequence) > 0 {
				alleles++
			}
		}
		if alleles == 2 {
			coverage++
		}
	}
	if coverage < cmd.minCoverage {
		return nil, nil
	}
	// "observed" array for p-value calculation (training set
	// only)
	obs := make([][]bool, (maxv+1)*2) // 2 slices (hom + het) for each variant#
	// one-hot output (all samples)
	outcols := make([][]int8, (maxv+1)*2)
	for i := range obs {
		obs[i] = make([]bool, cmd.trainingSetSize)
		outcols[i] = make([]int8, len(cmd.cgnames))
	}
	for cgid, name := range cmd.cgnames {
		tsid := cmd.trainingSet[cgid]
		cgvars := cgs[name].Variants[tagoffset*2:]
		tv0, tv1 := remap[cgvars[0]], remap[cgvars[1]]
		for v := tileVariantID(1); v <= maxv; v++ {
			if tv0 == v && tv1 == v {
				if tsid >= 0 {
					obs[v*2][tsid] = true
				}
				outcols[v*2][cgid] = 1
			} else if tv0 == v || tv1 == v {
				if tsid >= 0 {
					obs[v*2+1][tsid] = true
				}
				outcols[v*2+1][cgid] = 1
			}
		}
	}
	var onehot [][]int8
	var xref []onehotXref
	for col := 2; col < len(obs); col++ {
		// col 0,1 correspond to tile variant 0, i.e.,
		// no-call; col 2,3 correspond to the most common
		// variant; so we (normally) start at col 4.
		if col < 4 && !cmd.includeVariant1 {
			continue
		}
		if col&1 == 0 && cmd.pvalueMinFrequency < 1 && homhet2maf(obs[col:col+2]) < cmd.pvalueMinFrequency {
			// Skip both columns (hom and het) if allele
			// frequency is below threshold
			col++
			continue
		}
		atomic.AddInt64(&cmd.pvalueCallCount, 1)
		p := cmd.pvalue(obs[col])
		if cmd.chi2PValue < 1 && !(p < cmd.chi2PValue) {
			continue
		}
		onehot = append(onehot, outcols[col])
		xref = append(xref, onehotXref{
			tag:     tag,
			variant: tileVariantID(col >> 1),
			hom:     col&1 == 0,
			pvalue:  p,
		})
	}
	return onehot, xref
}

func homhet2maf(onehot [][]bool) float64 {
	if len(onehot[0]) == 0 {
		return 0
	}
	n := 0
	for i := range onehot[0] {
		if onehot[0][i] {
			// hom
			n += 2
		} else if onehot[1][i] {
			// het
			n += 1
		}
	}
	return float64(n) / float64(len(onehot[0])*2)
}

// convert a []onehotXref with length N to a numpy-style []int32
// matrix with N columns, one row per field of onehotXref struct.
//
// Hom/het row contains hom=0, het=1.
//
// P-value row contains 1000000x actual p-value.
func onehotXref2int32(xrefs []onehotXref) []int32 {
	xcols := len(xrefs)
	xdata := make([]int32, 5*xcols)
	for i, xref := range xrefs {
		xdata[i] = int32(xref.tag)
		xdata[xcols+i] = int32(xref.variant)
		if xref.hom {
			xdata[xcols*2+i] = 1
		}
		xdata[xcols*3+i] = int32(xref.pvalue * 1000000)
		xdata[xcols*4+i] = int32(-math.Log10(xref.pvalue) * 1000000)
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
