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
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/blake2b"
)

type dump struct {
	filter       filter
	cgnames      []string
	selectedTags map[tagID]bool
}

func (cmd *dump) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := cmd.run(prog, args, stdin, stdout, stderr)
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *dump) run(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
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
	selectedTags := flags.String("tags", "", "tag numbers to dump")
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
		err = runner.TranslatePaths(inputDir, regionsFilename)
		if err != nil {
			return err
		}
		runner.Args = []string{"dump", "-local=true",
			"-pprof=:6060",
			"-input-dir=" + *inputDir,
			"-output-dir=/mnt/output",
			"-regions=" + *regionsFilename,
			"-expand-regions=" + fmt.Sprintf("%d", *expandRegions),
			"-tags=" + *selectedTags,
		}
		runner.Args = append(runner.Args, cmd.filter.Args()...)
		output, err := runner.Run()
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, output)
		return nil
	}

	if *selectedTags != "" {
		cmd.selectedTags = map[tagID]bool{}
		for _, tagstr := range strings.Split(*selectedTags, ",") {
			tag, err := strconv.ParseInt(tagstr, 10, 64)
			if err != nil {
				return err
			}
			cmd.selectedTags[tagID(tag)] = true
		}
	}

	infiles, err := allFiles(*inputDir, matchGobFile)
	if err != nil {
		return err
	}
	if len(infiles) == 0 {
		return fmt.Errorf("no input files found in %s", *inputDir)
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

	cmd.cgnames = nil
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
		return fmt.Errorf("%s: reference sequence not found", infiles[0])
	}
	if taglen < 0 {
		return fmt.Errorf("tagset not found")
	}
	if len(cmd.cgnames) == 0 {
		return fmt.Errorf("no genomes found matching regexp %q", cmd.filter.MatchGenome)
	}
	sort.Strings(cmd.cgnames)

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
				return fmt.Errorf("missing tiledata for tag %d variant %d in %s in ref", libref.Tag, libref.Variant, seqname)
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
			return err
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

	if cmd.selectedTags != nil {
		log.Printf("deleting reftile entries other than %d selected tags", len(cmd.selectedTags))
		for tag := range reftile {
			if !cmd.selectedTags[tag] {
				delete(reftile, tag)
			}
		}
		log.Printf("after applying selected tags, len(reftile) == %d", len(reftile))
	}

	dumpVariantsName := fmt.Sprintf("%s/variants.csv", *outputDir)
	log.Infof("writing %s", dumpVariantsName)
	dumpVariantsF, err := os.Create(dumpVariantsName)
	if err != nil {
		return err
	}
	dumpVariantsW := bufio.NewWriterSize(dumpVariantsF, 1<<20)
	mtx := sync.Mutex{}

	throttleMem := throttle{Max: runtime.GOMAXPROCS(0)}
	log.Infof("reading %d slices with max concurrency %d", len(infiles), throttleMem.Max)
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
				throttleCPU.Go(func() error {
					count := make(map[[blake2b.Size256]byte]int, len(variants))

					rt := reftile[tag]
					var rthash [blake2b.Size256]byte
					if rt != nil {
						rthash = blake2b.Sum256(rt.tiledata)
						count[rthash] = 0
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
						rt.variant = rank[rthash]
					}
					return nil
				})
			}
			throttleCPU.Wait()

			for tag := tagstart; tag < tagend; tag++ {
				rt, ok := reftile[tag]
				if !ok {
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
				variants := seq[tag]

				mtx.Lock()
				fmt.Fprintf(dumpVariantsW, "%d,%d,1,%s,%d,%s\n", tag, rt.variant, rt.seqname, rt.pos+1, bytes.ToUpper(rt.tiledata))
				mtx.Unlock()

				done := make([]bool, maxv+1)
				for v, tv := range variants {
					v := remap[v]
					if v == 0 || v == rt.variant || done[v] {
						continue
					} else {
						done[v] = true
					}
					mtx.Lock()
					fmt.Fprintf(dumpVariantsW, "%d,%d,0,%s,%d,%s\n", tag, v, rt.seqname, rt.pos+1, bytes.ToUpper(tv.Sequence))
					mtx.Unlock()
				}
			}
			log.Infof("%s: done (%d/%d)", infile, int(atomic.AddInt64(&done, 1)), len(infiles))
			return nil
		})
	}
	if err = throttleMem.Wait(); err != nil {
		return err
	}
	err = dumpVariantsW.Flush()
	if err != nil {
		return err
	}
	err = dumpVariantsF.Close()
	if err != nil {
		return err
	}
	return nil
}
