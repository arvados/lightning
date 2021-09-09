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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
	log "github.com/sirupsen/logrus"
)

type annotatecmd struct {
	dropTiles        []bool
	variantHash      bool
	maxTileSize      int
	tag2tagid        map[string]tagID
	reportAnnotation func(tag tagID, outcol int, variant tileVariantID, refname string, seqname string, pdi hgvs.Variant)
}

func (cmd *annotatecmd) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	inputFilename := flags.String("i", "-", "input `file` (library)")
	outputFilename := flags.String("o", "-", "output `file`")
	flags.BoolVar(&cmd.variantHash, "variant-hash", false, "output variant hash instead of index")
	flags.IntVar(&cmd.maxTileSize, "max-tile-size", 50000, "don't try to make annotations for tiles bigger than given `size`")
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
			Name:        "lightning annotate",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         80000000000,
			VCPUs:       16,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"annotate", "-local=true", fmt.Sprintf("-variant-hash=%v", cmd.variantHash), "-max-tile-size", strconv.Itoa(cmd.maxTileSize), "-i", *inputFilename, "-o", "/mnt/output/tilevariants.csv"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/tilevariants.csv")
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

	var output io.WriteCloser
	if *outputFilename == "-" {
		output = nopCloser{stdout}
	} else {
		output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
		defer output.Close()
	}
	bufw := bufio.NewWriterSize(output, 4*1024*1024)

	tilelib := &tileLibrary{
		retainNoCalls:       true,
		retainTileSequences: true,
	}
	err = tilelib.LoadGob(context.Background(), input, strings.HasSuffix(*inputFilename, ".gz"))
	if err != nil {
		return 1
	}
	err = cmd.exportTileDiffs(bufw, tilelib)
	if err != nil {
		return 1
	}
	err = bufw.Flush()
	if err != nil {
		return 1
	}
	err = output.Close()
	if err != nil {
		return 1
	}
	err = input.Close()
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *annotatecmd) exportTileDiffs(outw io.Writer, tilelib *tileLibrary) error {
	tagset := tilelib.taglib.Tags()
	if len(tagset) == 0 {
		return errors.New("cannot annotate library without tags")
	}
	taglen := len(tagset[0])
	var refs []string
	for name := range tilelib.refseqs {
		refs = append(refs, name)
	}
	cmd.tag2tagid = make(map[string]tagID, len(tagset))
	for tagid, tagseq := range tagset {
		cmd.tag2tagid[string(tagseq)] = tagID(tagid)
	}
	sort.Strings(refs)
	log.Infof("len(refs) %d", len(refs))

	outch := make(chan string, runtime.NumCPU()*2)
	var outwg sync.WaitGroup
	defer outwg.Wait()
	outwg.Add(1)
	go func() {
		defer outwg.Done()
		for s := range outch {
			io.WriteString(outw, s)
		}
	}()
	defer close(outch)

	nseqs := 0
	for _, refcs := range tilelib.refseqs {
		nseqs += len(refcs)
	}

	throttle := &throttle{Max: runtime.NumCPU()*2 + nseqs*2 + 1}
	defer throttle.Wait()

	for _, refname := range refs {
		refname := refname
		refcs := tilelib.refseqs[refname]
		var seqnames []string
		for seqname := range refcs {
			seqnames = append(seqnames, seqname)
		}
		sort.Strings(seqnames)
		for _, seqname := range seqnames {
			seqname := seqname
			throttle.Acquire()
			if throttle.Err() != nil {
				break
			}
			go func() {
				defer throttle.Release()
				throttle.Report(cmd.annotateSequence(throttle, outch, tilelib, taglen, refname, seqname, refcs[seqname], len(refs) > 1))
			}()
		}
	}
	throttle.Wait()
	return throttle.Err()
}

func (cmd *annotatecmd) annotateSequence(throttle *throttle, outch chan<- string, tilelib *tileLibrary, taglen int, refname, seqname string, reftiles []tileLibRef, refnamecol bool) error {
	refnamefield := ""
	if refnamecol {
		refnamefield = "," + trimFilenameForLabel(refname)
	}
	var refseq []byte
	// tilestart[123] is the index into refseq
	// where the tile for tag 123 was placed.
	tilestart := map[tagID]int{}
	tileend := map[tagID]int{}
	for _, libref := range reftiles {
		if libref.Variant < 1 {
			return fmt.Errorf("reference %q seq %q uses variant zero at tag %d", refname, seqname, libref.Tag)
		}
		seq := tilelib.TileVariantSequence(libref)
		if len(seq) < taglen {
			return fmt.Errorf("reference %q seq %q uses tile %d variant %d with sequence len %d < taglen %d", refname, seqname, libref.Tag, libref.Variant, len(seq), taglen)
		}
		overlap := taglen
		if len(refseq) == 0 {
			overlap = 0
		}
		tilestart[libref.Tag] = len(refseq) - overlap
		refseq = append(refseq, seq[overlap:]...)
		tileend[libref.Tag] = len(refseq)
	}
	log.Infof("seq %s len(refseq) %d len(tilestart) %d", seqname, len(refseq), len(tilestart))
	// outtag is tag's index in the subset of tags that aren't
	// dropped. If there are 10M tags and half are dropped by
	// dropTiles, tag ranges from 0 to 10M-1 and outtag ranges
	// from 0 to 5M-1.
	//
	// IOW, in the matrix built by cgs2array(), {tag} is
	// represented by columns {outtag}*2 and {outtag}*2+1.
	outcol := -1
	for tag, tvs := range tilelib.variant {
		if len(cmd.dropTiles) > tag && cmd.dropTiles[tag] {
			continue
		}
		tag := tagID(tag)
		outcol++
		// Must shadow outcol var to use safely in goroutine below.
		outcol := outcol
		refstart, ok := tilestart[tag]
		if !ok {
			// Tag didn't place on this reference
			// sequence. (It might place on the same
			// chromosome in a genome anyway, but we don't
			// output the annotations that would result.)
			// outch <- fmt.Sprintf("%d,%d,-1%s\n", tag, outcol, refnamefield)
			continue
		}
		for variant := 1; variant <= len(tvs); variant++ {
			variant, hash := tileVariantID(variant), tvs[variant-1]
			tileseq := tilelib.TileVariantSequence(tileLibRef{Tag: tag, Variant: variant})
			if len(tileseq) == 0 {
				continue
			} else if len(tileseq) < taglen {
				return fmt.Errorf("tilevar %d,%d has sequence len %d < taglen %d", tag, variant, len(tileseq), taglen)
			}
			var refpart []byte
			endtag := string(tileseq[len(tileseq)-taglen:])
			if endtagid, ok := cmd.tag2tagid[endtag]; !ok {
				// Tile variant doesn't end on a tag, so it can only place at the end of a chromosome.
				refpart = refseq[refstart:]
				log.Warnf("%x tilevar %d,%d endtag not in ref: %s", hash[:13], tag, variant, endtag)
			} else if refendtagstart, ok := tilestart[endtagid]; !ok {
				// Ref ends a chromsome with a (possibly very large) variant of this tile, but genomes with this tile don't.
				// Give up. (TODO: something smarter)
				log.Debugf("%x not annotating tilevar %d,%d because end tag %d is not in ref", hash[:13], tag, variant, endtagid)
				continue
			} else {
				// Non-terminal tile vs. non-terminal reference.
				refpart = refseq[refstart : refendtagstart+taglen]
				log.Tracef("\n%x tilevar %d,%d endtag %s endtagid %d refendtagstart %d", hash[:13], tag, variant, endtag, endtagid, refendtagstart)
			}
			if len(refpart) > cmd.maxTileSize {
				log.Warnf("%x tilevar %d,%d skipping long diff, ref %s seq %s pos %d ref len %d", hash[:13], tag, variant, refname, seqname, refstart, len(refpart))
				continue
			}
			if len(tileseq) > cmd.maxTileSize {
				log.Warnf("%x tilevar %d,%d skipping long diff, ref %s seq %s variant len %d", hash[:13], tag, variant, refname, seqname, len(tileseq))
				continue
			}
			// log.Printf("\n%x @ refstart %d \n< %s\n> %s\n", tv.Blake2b, refstart, refpart, tileseq)

			throttle.Acquire()
			go func() {
				defer throttle.Release()
				diffs, _ := hgvs.Diff(strings.ToUpper(string(refpart)), strings.ToUpper(string(tileseq)), 0)
				for _, diff := range diffs {
					diff.Position += refstart
					var varid string
					if cmd.variantHash {
						varid = fmt.Sprintf("%x", hash)[:13]
					} else {
						varid = fmt.Sprintf("%d", variant)
					}
					outch <- fmt.Sprintf("%d,%d,%s%s,%s:g.%s\n", tag, outcol, varid, refnamefield, seqname, diff.String())
					if cmd.reportAnnotation != nil {
						cmd.reportAnnotation(tag, outcol, variant, refname, seqname, diff)
					}
				}
			}()
		}
	}
	return nil
}
