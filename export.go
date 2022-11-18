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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
	"github.com/klauspost/pgzip"
	"github.com/kshedden/gonpy"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type tvVariant struct {
	hgvs.Variant
	librefs map[tileLibRef]bool
}

type outputFormat interface {
	Filename() string
	PadLeft() bool
	Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error
	Print(out io.Writer, seqname string, varslice []tvVariant) error
	Finish(outdir string, out io.Writer, seqname string) error
	MaxGoroutines() int
}

var outputFormats = map[string]func() outputFormat{
	"hgvs-numpy": func() outputFormat {
		return &formatHGVSNumpy{alleles: map[string][][]int8{}}
	},
	"hgvs-onehot": func() outputFormat { return formatHGVSOneHot{} },
	"hgvs":        func() outputFormat { return formatHGVS{} },
	"pvcf":        func() outputFormat { return formatPVCF{} },
	"vcf":         func() outputFormat { return formatVCF{} },
}

type exporter struct {
	outputFormat   outputFormat
	outputPerChrom bool
	compress       bool
	maxTileSize    int
	filter         filter
	maxPValue      float64
	cases          []bool
}

func (cmd *exporter) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	pprofdir := flags.String("pprof-dir", "", "write Go profile data to `directory` periodically")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	refname := flags.String("ref", "", "reference genome `name`")
	inputDir := flags.String("input-dir", ".", "input `directory`")
	cases := flags.String("cases", "", "file indicating which genomes are positive cases (for computing p-values)")
	flags.Float64Var(&cmd.maxPValue, "p-value", 1, "do chi square test and omit columns with p-value above this threshold")
	outputDir := flags.String("output-dir", ".", "output `directory`")
	outputFormatStr := flags.String("output-format", "hgvs", "output `format`: hgvs, pvcf, or vcf")
	outputBed := flags.String("output-bed", "", "also output bed `file`")
	flags.BoolVar(&cmd.outputPerChrom, "output-per-chromosome", true, "output one file per chromosome")
	flags.BoolVar(&cmd.compress, "z", false, "write gzip-compressed output files")
	labelsFilename := flags.String("output-labels", "", "also output genome labels csv `file`")
	flags.IntVar(&cmd.maxTileSize, "max-tile-size", 50000, "don't try to make annotations for tiles bigger than given `size`")
	cmd.filter.Flags(flags)
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if flags.NArg() > 0 {
		err = fmt.Errorf("errant command line arguments after parsed flags: %v", flags.Args())
		return 2
	}

	if f, ok := outputFormats[*outputFormatStr]; !ok {
		err = fmt.Errorf("invalid output format %q", *outputFormatStr)
		return 2
	} else {
		cmd.outputFormat = f()
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}
	if *pprofdir != "" {
		go writeProfilesPeriodically(*pprofdir)
	}

	if !*runlocal {
		if *outputDir != "." {
			err = errors.New("cannot specify output directory in container mode: not implemented")
			return 1
		}
		runner := arvadosContainerRunner{
			Name:        "lightning export",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         750000000000,
			VCPUs:       96,
			Priority:    *priority,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir, cases)
		if err != nil {
			return 1
		}
		if *outputBed != "" {
			if strings.Contains(*outputBed, "/") {
				err = fmt.Errorf("cannot use -output-bed filename %q containing '/' char", *outputBed)
				return 1
			}
			*outputBed = "/mnt/output/" + *outputBed
		}
		runner.Args = []string{"export", "-local=true",
			"-pprof", ":6000",
			"-pprof-dir", "/mnt/output",
			"-ref", *refname,
			"-cases", *cases,
			"-p-value", fmt.Sprintf("%f", cmd.maxPValue),
			"-output-format", *outputFormatStr,
			"-output-bed", *outputBed,
			"-output-labels", "/mnt/output/labels.csv",
			"-output-per-chromosome=" + fmt.Sprintf("%v", cmd.outputPerChrom),
			"-max-tile-size", fmt.Sprintf("%d", cmd.maxTileSize),
			"-input-dir", *inputDir,
			"-output-dir", "/mnt/output",
			"-z=" + fmt.Sprintf("%v", cmd.compress),
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

	var cgs []CompactGenome
	tilelib := &tileLibrary{
		retainNoCalls:       true,
		retainTileSequences: true,
		compactGenomes:      map[string][]tileVariantID{},
	}
	err = tilelib.LoadDir(context.Background(), *inputDir)
	if err != nil {
		return 1
	}

	refseq, ok := tilelib.refseqs[*refname]
	if !ok {
		err = fmt.Errorf("reference name %q not found in input; have %v", *refname, func() (names []string) {
			for name := range tilelib.refseqs {
				names = append(names, name)
			}
			return
		}())
		return 1
	}

	log.Infof("filtering: %+v", cmd.filter)
	cmd.filter.Apply(tilelib)

	names := cgnames(tilelib)
	for _, name := range names {
		cgs = append(cgs, CompactGenome{Name: name, Variants: tilelib.compactGenomes[name]})
	}
	if *labelsFilename != "" {
		log.Infof("writing labels to %s", *labelsFilename)
		var f *os.File
		f, err = os.OpenFile(*labelsFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer f.Close()
		for i, name := range names {
			_, err = fmt.Fprintf(f, "%d,%q,%q\n", i, trimFilenameForLabel(name), cmd.outputFormat.Filename())
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

	cmd.cases = make([]bool, len(names))
	if *cases != "" {
		log.Infof("reading cases file: %s", *cases)
		var f io.ReadCloser
		f, err = open(*cases)
		if err != nil {
			return 1
		}
		defer f.Close()
		var buf []byte
		buf, err = io.ReadAll(f)
		if err != nil {
			return 1
		}
		for _, pattern := range bytes.Split(buf, []byte("\n")) {
			if len(pattern) == 0 {
				continue
			}
			pattern := string(pattern)
			idx := -1
			for i, name := range names {
				if !strings.Contains(name, pattern) {
					continue
				} else if idx >= 0 {
					err = fmt.Errorf("pattern %q in cases file matches multiple genome IDs: %q, %q", pattern, names[idx], name)
					return 1
				} else {
					idx = i
				}
			}
			if idx < 0 {
				log.Warnf("pattern %q in cases file does not match any genome IDs", pattern)
				continue
			}
			cmd.cases[idx] = true
		}
	}

	var bedout io.Writer
	var bedfile *os.File
	var bedbufw *bufio.Writer
	if *outputBed != "" {
		bedfile, err = os.OpenFile(*outputBed, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
		defer bedfile.Close()
		bedbufw = bufio.NewWriterSize(bedfile, 16*1024*1024)
		bedout = bedbufw
	}

	err = cmd.export(*outputDir, bedout, tilelib, refseq, cgs)
	if err != nil {
		return 1
	}
	if bedout != nil {
		err = bedbufw.Flush()
		if err != nil {
			return 1
		}
		err = bedfile.Close()
		if err != nil {
			return 1
		}
	}
	return 0
}

func (cmd *exporter) export(outdir string, bedout io.Writer, tilelib *tileLibrary, refseq map[string][]tileLibRef, cgs []CompactGenome) error {
	var seqnames []string
	var missing []tileLibRef
	for seqname, librefs := range refseq {
		seqnames = append(seqnames, seqname)
		for _, libref := range librefs {
			if libref.Variant != 0 && tilelib.TileVariantSequence(libref) == nil {
				missing = append(missing, libref)
			}
		}
	}
	sort.Strings(seqnames)

	if len(missing) > 0 {
		if limit := 100; len(missing) > limit {
			log.Warnf("first %d missing tiles: %v", limit, missing[:limit])
		} else {
			log.Warnf("missing tiles: %v", missing)
		}
		return fmt.Errorf("%d needed tiles are missing from library", len(missing))
	}

	outw := make([]io.WriteCloser, len(seqnames))
	bedw := make([]io.WriteCloser, len(seqnames))

	var merges sync.WaitGroup
	merge := func(dst io.Writer, src []io.WriteCloser, label string) {
		var mtx sync.Mutex
		for i, seqname := range seqnames {
			pr, pw := io.Pipe()
			src[i] = pw
			merges.Add(1)
			seqname := seqname
			go func() {
				defer merges.Done()
				log.Infof("writing %s %s", seqname, label)
				scanner := bufio.NewScanner(pr)
				for scanner.Scan() {
					mtx.Lock()
					dst.Write(scanner.Bytes())
					dst.Write([]byte{'\n'})
					mtx.Unlock()
				}
				log.Infof("writing %s %s done", seqname, label)
			}()
		}
	}
	if cmd.outputPerChrom {
		for i, seqname := range seqnames {
			fnm := filepath.Join(outdir, strings.Replace(cmd.outputFormat.Filename(), ".", "."+seqname+".", 1))
			if cmd.compress {
				fnm += ".gz"
			}
			f, err := os.OpenFile(fnm, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			defer f.Close()
			log.Infof("writing %q", f.Name())
			outw[i] = f
			if cmd.compress {
				z := pgzip.NewWriter(f)
				defer z.Close()
				outw[i] = z
			}
			err = cmd.outputFormat.Head(outw[i], cgs, cmd.cases, cmd.maxPValue)
			if err != nil {
				return err
			}
		}
	} else {
		fnm := filepath.Join(outdir, cmd.outputFormat.Filename())
		if cmd.compress {
			fnm += ".gz"
		}
		f, err := os.OpenFile(fnm, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		log.Infof("writing %q", fnm)
		var out io.Writer = f
		if cmd.compress {
			z := pgzip.NewWriter(out)
			defer z.Close()
			out = z
		}
		cmd.outputFormat.Head(out, cgs, cmd.cases, cmd.maxPValue)
		merge(out, outw, "output")
	}
	if bedout != nil {
		merge(bedout, bedw, "bed")
	}

	throttle := throttle{Max: runtime.NumCPU()}
	if max := cmd.outputFormat.MaxGoroutines(); max > 0 {
		throttle.Max = max
	}
	log.Infof("assembling %d sequences in %d goroutines", len(seqnames), throttle.Max)
	for seqidx, seqname := range seqnames {
		seqidx, seqname := seqidx, seqname
		outw := outw[seqidx]
		bedw := bedw[seqidx]
		throttle.Acquire()
		go func() {
			defer throttle.Release()
			if bedw != nil {
				defer bedw.Close()
			}
			outwb := bufio.NewWriterSize(outw, 8*1024*1024)
			eachVariant(bedw, tilelib.taglib.keylen, seqname, refseq[seqname], tilelib, cgs, cmd.outputFormat.PadLeft(), cmd.maxTileSize, func(varslice []tvVariant) {
				err := cmd.outputFormat.Print(outwb, seqname, varslice)
				throttle.Report(err)
			})
			err := cmd.outputFormat.Finish(outdir, outwb, seqname)
			throttle.Report(err)
			err = outwb.Flush()
			throttle.Report(err)
			err = outw.Close()
			throttle.Report(err)
		}()
	}

	merges.Wait()
	throttle.Wait()
	return throttle.Err()
}

// Align genome tiles to reference tiles, call callback func on each
// variant, and (if bedw is not nil) write tile coverage to bedw.
func eachVariant(bedw io.Writer, taglen int, seqname string, reftiles []tileLibRef, tilelib *tileLibrary, cgs []CompactGenome, padLeft bool, maxTileSize int, callback func(varslice []tvVariant)) {
	t0 := time.Now()
	progressbar := time.NewTicker(time.Minute)
	defer progressbar.Stop()
	var outmtx sync.Mutex
	defer outmtx.Lock()
	refpos := 0
	variantAt := map[int][]tvVariant{} // variantAt[chromOffset][genomeIndex*2+phase]
	for refstep, libref := range reftiles {
		select {
		case <-progressbar.C:
			var eta interface{}
			if refstep > 0 {
				fin := t0.Add(time.Duration(float64(time.Now().Sub(t0)) * float64(len(reftiles)) / float64(refstep)))
				eta = fmt.Sprintf("%v (%v)", fin.Format(time.RFC3339), fin.Sub(time.Now()))
			} else {
				eta = "N/A"
			}
			log.Printf("exportSeq: %s: refstep %d of %d, %.0f/s, ETA %v", seqname, refstep, len(reftiles), float64(refstep)/time.Now().Sub(t0).Seconds(), eta)
		default:
		}
		diffs := map[tileLibRef][]hgvs.Variant{}
		refseq := tilelib.TileVariantSequence(libref)
		tagcoverage := 0 // number of times the start tag was found in genomes -- max is len(cgs)*2
		for cgidx, cg := range cgs {
			for phase := 0; phase < 2; phase++ {
				var variant tileVariantID
				if i := int(libref.Tag)*2 + phase; len(cg.Variants) > i {
					variant = cg.Variants[i]
				}
				if variant > 0 {
					tagcoverage++
				}
				if variant == libref.Variant || variant == 0 {
					continue
				}
				glibref := tileLibRef{Tag: libref.Tag, Variant: variant}
				vars, ok := diffs[glibref]
				if !ok {
					genomeseq := tilelib.TileVariantSequence(glibref)
					if len(genomeseq) == 0 {
						// Hash is known but sequence
						// is not, e.g., retainNoCalls
						// was false during import
						continue
					}
					if len(genomeseq) > maxTileSize {
						continue
					}
					refSequence := refseq
					// If needed, extend the
					// reference sequence up to
					// the tag at the end of the
					// genomeseq sequence.
					refstepend := refstep + 1
					for refstepend < len(reftiles) && len(refSequence) >= taglen && !bytes.EqualFold(refSequence[len(refSequence)-taglen:], genomeseq[len(genomeseq)-taglen:]) && len(refSequence) <= maxTileSize {
						if &refSequence[0] == &refseq[0] {
							refSequence = append([]byte(nil), refSequence...)
						}
						refSequence = append(refSequence, tilelib.TileVariantSequence(reftiles[refstepend])...)
						refstepend++
					}
					// (TODO: handle no-calls)
					if len(refSequence) <= maxTileSize {
						refstr := strings.ToUpper(string(refSequence))
						genomestr := strings.ToUpper(string(genomeseq))
						vars, _ = hgvs.Diff(refstr, genomestr, time.Second)
					}
					diffs[glibref] = vars
				}
				for _, v := range vars {
					if padLeft {
						v = v.PadLeft()
					}
					v.Position += refpos
					varslice := variantAt[v.Position]
					if varslice == nil {
						varslice = make([]tvVariant, len(cgs)*2)
						variantAt[v.Position] = varslice
					}
					varslice[cgidx*2+phase].Variant = v
					if varslice[cgidx*2+phase].librefs == nil {
						varslice[cgidx*2+phase].librefs = map[tileLibRef]bool{glibref: true}
					} else {
						varslice[cgidx*2+phase].librefs[glibref] = true
					}
				}
			}
		}
		refpos += len(refseq) - taglen

		// Flush entries from variantAt that are behind
		// refpos. Flush all entries if this is the last
		// reftile of the path/chromosome.
		flushpos := make([]int, 0, len(variantAt))
		lastrefstep := refstep == len(reftiles)-1
		for pos := range variantAt {
			if lastrefstep || pos <= refpos {
				flushpos = append(flushpos, pos)
			}
		}
		sort.Slice(flushpos, func(i, j int) bool { return flushpos[i] < flushpos[j] })
		flushvariants := make([][]tvVariant, len(flushpos))
		for i, pos := range flushpos {
			varslice := variantAt[pos]
			delete(variantAt, pos)
			// Check for uninitialized (zero-value)
			// elements in varslice
			for i := range varslice {
				if varslice[i].Position != 0 {
					// Not a zero-value element
					continue
				}
				// Set the position so
				// varslice[*].Position are all equal
				varslice[i].Position = pos
				// This could be either =ref or a
				// missing/low-quality tile. Figure
				// out which.
				vidx := int(libref.Tag)*2 + i%2
				if vidx >= len(cgs[i/2].Variants) {
					// Missing tile.
					varslice[i].New = "-"
					continue
				}
				v := cgs[i/2].Variants[vidx]
				if v < 1 || len(tilelib.TileVariantSequence(tileLibRef{Tag: libref.Tag, Variant: v})) == 0 {
					// Missing/low-quality tile.
					varslice[i].New = "-" // fasta "gap of indeterminate length"
				}
			}
			flushvariants[i] = varslice
		}
		outmtx.Lock()
		go func() {
			defer outmtx.Unlock()
			for _, varslice := range flushvariants {
				callback(varslice)
			}
		}()
		if bedw != nil && len(refseq) > 0 {
			tilestart := refpos - len(refseq) + taglen
			tileend := refpos
			if !lastrefstep {
				tileend += taglen
			}
			thickstart := tilestart + taglen
			if refstep == 0 {
				thickstart = 0
			}
			thickend := refpos

			// coverage score, 0 to 1000
			score := 1000
			if len(cgs) > 0 {
				score = 1000 * tagcoverage / len(cgs) / 2
			}

			fmt.Fprintf(bedw, "%s %d %d %d %d . %d %d\n",
				seqname, tilestart, tileend,
				libref.Tag,
				score,
				thickstart, thickend)
		}
	}
}

func bucketVarsliceByRef(varslice []tvVariant) map[string]map[string]int {
	byref := map[string]map[string]int{}
	for _, v := range varslice {
		if v.Ref == "" && v.New == "" {
			// =ref
			continue
		}
		if v.New == "-" {
			// no-call
			continue
		}
		alts := byref[v.Ref]
		if alts == nil {
			alts = map[string]int{}
			byref[v.Ref] = alts
		}
		alts[v.New]++
	}
	return byref
}

type formatVCF struct{}

func (formatVCF) MaxGoroutines() int                     { return 0 }
func (formatVCF) Filename() string                       { return "out.vcf" }
func (formatVCF) PadLeft() bool                          { return true }
func (formatVCF) Finish(string, io.Writer, string) error { return nil }
func (formatVCF) Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error {
	_, err := fmt.Fprint(out, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
	return err
}
func (formatVCF) Print(out io.Writer, seqname string, varslice []tvVariant) error {
	for ref, alts := range bucketVarsliceByRef(varslice) {
		altslice := make([]string, 0, len(alts))
		for alt := range alts {
			altslice = append(altslice, alt)
		}
		sort.Strings(altslice)

		info := "AC="
		for i, a := range altslice {
			if i > 0 {
				info += ","
			}
			info += strconv.Itoa(alts[a])
		}
		_, err := fmt.Fprintf(out, "%s\t%d\t.\t%s\t%s\t.\t.\t%s\n", seqname, varslice[0].Position, ref, strings.Join(altslice, ","), info)
		if err != nil {
			return err
		}
	}
	return nil
}

type formatPVCF struct{}

func (formatPVCF) MaxGoroutines() int                     { return 0 }
func (formatPVCF) Filename() string                       { return "out.vcf" }
func (formatPVCF) PadLeft() bool                          { return true }
func (formatPVCF) Finish(string, io.Writer, string) error { return nil }
func (formatPVCF) Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error {
	fmt.Fprintln(out, `##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">`)
	fmt.Fprintf(out, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
	for _, cg := range cgs {
		fmt.Fprintf(out, "\t%s", cg.Name)
	}
	_, err := fmt.Fprintf(out, "\n")
	return err
}

func (formatPVCF) Print(out io.Writer, seqname string, varslice []tvVariant) error {
	for ref, alts := range bucketVarsliceByRef(varslice) {
		altslice := make([]string, 0, len(alts))
		for alt := range alts {
			altslice = append(altslice, alt)
		}
		sort.Strings(altslice)
		for i, a := range altslice {
			alts[a] = i + 1
		}
		_, err := fmt.Fprintf(out, "%s\t%d\t.\t%s\t%s\t.\t.\t.\tGT", seqname, varslice[0].Position, ref, strings.Join(altslice, ","))
		if err != nil {
			return err
		}
		for i := 0; i < len(varslice); i += 2 {
			v1, v2 := varslice[i], varslice[i+1]
			a1, a2 := alts[v1.New], alts[v2.New]
			if v1.Ref != ref {
				// variant on allele 0 belongs on a
				// different output line -- same
				// chr,pos but different "ref" length
				a1 = 0
			}
			if v2.Ref != ref {
				a2 = 0
			}
			_, err := fmt.Fprintf(out, "\t%d/%d", a1, a2)
			if err != nil {
				return err
			}
		}
		_, err = out.Write([]byte{'\n'})
		if err != nil {
			return err
		}
	}
	return nil
}

type formatHGVS struct{}

func (formatHGVS) MaxGoroutines() int                                                     { return 0 }
func (formatHGVS) Filename() string                                                       { return "out.tsv" }
func (formatHGVS) PadLeft() bool                                                          { return false }
func (formatHGVS) Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error { return nil }
func (formatHGVS) Finish(string, io.Writer, string) error                                 { return nil }
func (formatHGVS) Print(out io.Writer, seqname string, varslice []tvVariant) error {
	for i := 0; i < len(varslice)/2; i++ {
		if i > 0 {
			out.Write([]byte{'\t'})
		}
		var1, var2 := varslice[i*2], varslice[i*2+1]
		if var1.New == "-" || var2.New == "-" {
			_, err := out.Write([]byte{'N'})
			if err != nil {
				return err
			}
			continue
		}
		if var1.Variant == var2.Variant {
			if var1.Ref == var1.New {
				_, err := out.Write([]byte{'.'})
				if err != nil {
					return err
				}
			} else {
				_, err := fmt.Fprintf(out, "%s:g.%s", seqname, var1.String())
				if err != nil {
					return err
				}
			}
		} else {
			_, err := fmt.Fprintf(out, "%s:g.[%s];[%s]", seqname, var1.String(), var2.String())
			if err != nil {
				return err
			}
		}
	}
	_, err := out.Write([]byte{'\n'})
	return err
}

type formatHGVSOneHot struct{}

func (formatHGVSOneHot) MaxGoroutines() int { return 0 }
func (formatHGVSOneHot) Filename() string   { return "out.tsv" }
func (formatHGVSOneHot) PadLeft() bool      { return false }
func (formatHGVSOneHot) Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error {
	return nil
}
func (formatHGVSOneHot) Finish(string, io.Writer, string) error { return nil }
func (formatHGVSOneHot) Print(out io.Writer, seqname string, varslice []tvVariant) error {
	vars := map[hgvs.Variant]bool{}
	for _, v := range varslice {
		if v.Ref != v.New {
			vars[v.Variant] = true
		}
	}

	// sort variants to ensure output is deterministic
	sorted := make([]hgvs.Variant, 0, len(vars))
	for v := range vars {
		sorted = append(sorted, v)
	}
	sort.Slice(sorted, func(a, b int) bool { return hgvs.Less(sorted[a], sorted[b]) })

	for _, v := range sorted {
		if v.New == "-" {
			continue
		}
		fmt.Fprintf(out, "%s.%s", seqname, v.String())
		for i := 0; i < len(varslice); i += 2 {
			if varslice[i].Variant == v || varslice[i+1].Variant == v {
				out.Write([]byte("\t1"))
			} else {
				out.Write([]byte("\t0"))
			}
		}
		_, err := out.Write([]byte{'\n'})
		if err != nil {
			return err
		}
	}
	return nil
}

type formatHGVSNumpy struct {
	sync.Mutex
	writelock sync.Mutex
	alleles   map[string][][]int8 // alleles[seqname][variantidx][genomeidx*2+phase]
	cases     []bool
	maxPValue float64
}

func (*formatHGVSNumpy) MaxGoroutines() int { return 4 }
func (*formatHGVSNumpy) Filename() string   { return "annotations.csv" }
func (*formatHGVSNumpy) PadLeft() bool      { return false }
func (f *formatHGVSNumpy) Head(out io.Writer, cgs []CompactGenome, cases []bool, p float64) error {
	f.cases = cases
	f.maxPValue = p
	return nil
}
func (f *formatHGVSNumpy) Print(outw io.Writer, seqname string, varslice []tvVariant) error {
	// sort variants to ensure output is deterministic
	sorted := make([]hgvs.Variant, 0, len(varslice))
	for _, v := range varslice {
		sorted = append(sorted, v.Variant)
	}
	sort.Slice(sorted, func(a, b int) bool { return hgvs.Less(sorted[a], sorted[b]) })

	f.Lock()
	seqalleles := f.alleles[seqname]
	f.Unlock()

	chi2x := make([]bool, 0, len(varslice))
	chi2y := make([]bool, 0, len(varslice))

	// append a row to seqalleles for each unique non-ref variant
	// in varslice.
	var previous hgvs.Variant
	for _, v := range sorted {
		if previous == v || v.Ref == v.New || v.New == "-" {
			continue
		}
		previous = v
		chi2x, chi2y := chi2x, chi2y
		newrow := make([]int8, len(varslice))
		for i, allele := range varslice {
			if allele.Variant == v {
				newrow[i] = 1
				chi2x = append(chi2x, true)
				chi2y = append(chi2y, f.cases[i/2])
			} else if allele.Variant.New == "-" {
				newrow[i] = -1
			} else {
				chi2x = append(chi2x, false)
				chi2y = append(chi2y, f.cases[i/2])
			}
		}
		if f.maxPValue < 1 && pvalue(chi2x, chi2y) > f.maxPValue {
			continue
		}
		seqalleles = append(seqalleles, newrow)
		_, err := fmt.Fprintf(outw, "%d,%q\n", len(seqalleles)-1, seqname+"."+v.String())
		if err != nil {
			return err
		}
	}

	f.Lock()
	f.alleles[seqname] = seqalleles
	f.Unlock()
	return nil
}
func (f *formatHGVSNumpy) Finish(outdir string, _ io.Writer, seqname string) error {
	// Write seqname's data to a .npy matrix with one row per
	// genome and 2 columns per variant.
	f.Lock()
	seqalleles := f.alleles[seqname]
	delete(f.alleles, seqname)
	f.Unlock()
	if len(seqalleles) == 0 {
		return nil
	}
	out := make([]int8, len(seqalleles)*len(seqalleles[0]))
	rows := len(seqalleles[0]) / 2
	cols := len(seqalleles) * 2
	// copy seqalleles[varidx][genome*2+phase] to
	// out[genome*nvars*2 + varidx*2 + phase]
	for varidx, alleles := range seqalleles {
		for g := 0; g < len(alleles)/2; g++ {
			aa, ab := alleles[g*2], alleles[g*2+1]
			if aa < 0 || ab < 0 {
				// no-call
				out[g*cols+varidx*2] = -1
				out[g*cols+varidx*2+1] = -1
			} else if aa > 0 && ab > 0 {
				// hom
				out[g*cols+varidx*2] = 1
			} else if aa > 0 || ab > 0 {
				// het
				out[g*cols+varidx*2+1] = 1
			}
		}
	}
	outf, err := os.OpenFile(outdir+"/matrix."+seqname+".npy", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer outf.Close()
	bufw := bufio.NewWriter(outf)
	npw, err := gonpy.NewWriter(nopCloser{bufw})
	if err != nil {
		return err
	}
	log.WithFields(logrus.Fields{
		"seqname": seqname,
		"rows":    rows,
		"cols":    cols,
	}).Info("writing numpy")
	npw.Shape = []int{rows, cols}
	f.writelock.Lock() // serialize because WriteInt8 uses lots of memory
	npw.WriteInt8(out)
	f.writelock.Unlock()
	err = bufw.Flush()
	if err != nil {
		return err
	}
	err = outf.Close()
	if err != nil {
		return err
	}
	return nil
}
