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
	log "github.com/sirupsen/logrus"
)

type outputFormat struct {
	Filename string
	Print    func(out io.Writer, seqname string, varslice []hgvs.Variant)
	PadLeft  bool
}

var (
	outputFormats = map[string]outputFormat{
		"hgvs-onehot": outputFormatHGVSOneHot,
		"hgvs":        outputFormatHGVS,
		"pvcf":        outputFormatPVCF,
		"vcf":         outputFormatVCF,
	}
	outputFormatHGVS       = outputFormat{Filename: "out.csv", Print: printHGVS}
	outputFormatHGVSOneHot = outputFormat{Filename: "out.csv", Print: printHGVSOneHot}
	outputFormatPVCF       = outputFormat{Filename: "out.vcf", Print: printPVCF, PadLeft: true}
	outputFormatVCF        = outputFormat{Filename: "out.vcf", Print: printVCF, PadLeft: true}
)

type exporter struct {
	outputFormat   outputFormat
	outputPerChrom bool
	maxTileSize    int
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
	outputDir := flags.String("output-dir", ".", "output `directory`")
	outputFormatStr := flags.String("output-format", "hgvs", "output `format`: hgvs, pvcf, or vcf")
	outputBed := flags.String("output-bed", "", "also output bed `file`")
	flags.BoolVar(&cmd.outputPerChrom, "output-per-chromosome", true, "output one file per chromosome")
	labelsFilename := flags.String("output-labels", "", "also output genome labels csv `file`")
	flags.IntVar(&cmd.maxTileSize, "max-tile-size", 50000, "don't try to make annotations for tiles bigger than given `size`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	if flag.NArg() > 0 {
		err = fmt.Errorf("extra unparsed command line arguments: %q", flag.Args())
		return 2
	}

	if f, ok := outputFormats[*outputFormatStr]; !ok {
		err = fmt.Errorf("invalid output format %q", *outputFormatStr)
		return 2
	} else {
		cmd.outputFormat = f
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
			RAM:         700000000000,
			VCPUs:       96,
			Priority:    *priority,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir)
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
			"-output-format", *outputFormatStr,
			"-output-bed", *outputBed,
			"-output-labels", "/mnt/output/labels.csv",
			"-output-per-chromosome=" + fmt.Sprintf("%v", cmd.outputPerChrom),
			"-max-tile-size", fmt.Sprintf("%d", cmd.maxTileSize),
			"-input-dir", *inputDir,
			"-output-dir", "/mnt/output",
		}
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
	err = tilelib.LoadDir(context.Background(), *inputDir, nil)
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
			_, err = fmt.Fprintf(f, "%d,%q,%q\n", i, trimFilenameForLabel(name), cmd.outputFormat.Filename)
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
			f, err := os.OpenFile(filepath.Join(outdir, strings.Replace(cmd.outputFormat.Filename, ".", "."+seqname+".", 1)), os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				return err
			}
			defer f.Close()
			log.Infof("writing %q", f.Name())
			outw[i] = f
		}
	} else {
		fnm := filepath.Join(outdir, cmd.outputFormat.Filename)
		log.Infof("writing %q", fnm)
		out, err := os.OpenFile(fnm, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		defer out.Close()
		merge(out, outw, "output")
	}
	if bedout != nil {
		merge(bedout, bedw, "bed")
	}

	throttle := throttle{Max: runtime.NumCPU()}
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
			defer outw.Close()
			outwb := bufio.NewWriterSize(outw, 8*1024*1024)
			defer outwb.Flush()
			cmd.exportSeq(outwb, bedw, tilelib.taglib.keylen, seqname, refseq[seqname], tilelib, cgs)
		}()
	}

	merges.Wait()
	throttle.Wait()
	return nil
}

// Align genome tiles to reference tiles, write diffs to outw, and (if
// bedw is not nil) write tile coverage to bedw.
func (cmd *exporter) exportSeq(outw, bedw io.Writer, taglen int, seqname string, reftiles []tileLibRef, tilelib *tileLibrary, cgs []CompactGenome) {
	t0 := time.Now()
	progressbar := time.NewTicker(time.Minute)
	defer progressbar.Stop()
	var outmtx sync.Mutex
	defer outmtx.Lock()
	refpos := 0
	variantAt := map[int][]hgvs.Variant{} // variantAt[chromOffset][genomeIndex*2+phase]
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
				if len(cg.Variants) <= int(libref.Tag)*2+phase {
					continue
				}
				variant := cg.Variants[int(libref.Tag)*2+phase]
				if variant == 0 {
					continue
				}
				tagcoverage++
				if variant == libref.Variant {
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
					if len(genomeseq) > cmd.maxTileSize {
						continue
					}
					refSequence := refseq
					// If needed, extend the
					// reference sequence up to
					// the tag at the end of the
					// genomeseq sequence.
					refstepend := refstep + 1
					for refstepend < len(reftiles) && len(refSequence) >= taglen && !bytes.EqualFold(refSequence[len(refSequence)-taglen:], genomeseq[len(genomeseq)-taglen:]) && len(refSequence) <= cmd.maxTileSize {
						if &refSequence[0] == &refseq[0] {
							refSequence = append([]byte(nil), refSequence...)
						}
						refSequence = append(refSequence, tilelib.TileVariantSequence(reftiles[refstepend])...)
						refstepend++
					}
					// (TODO: handle no-calls)
					refstr := strings.ToUpper(string(refSequence))
					genomestr := strings.ToUpper(string(genomeseq))
					vars, _ = hgvs.Diff(refstr, genomestr, time.Second)
					diffs[glibref] = vars
				}
				for _, v := range vars {
					if cmd.outputFormat.PadLeft {
						v = v.PadLeft()
					}
					v.Position += refpos
					varslice := variantAt[v.Position]
					if varslice == nil {
						varslice = make([]hgvs.Variant, len(cgs)*2)
						variantAt[v.Position] = varslice
					}
					varslice[cgidx*2+phase] = v
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
		flushvariants := make([][]hgvs.Variant, len(flushpos))
		for i, pos := range flushpos {
			varslice := variantAt[pos]
			delete(variantAt, pos)
			for i := range varslice {
				if varslice[i].Position == 0 {
					varslice[i].Position = pos
				}
			}
			flushvariants[i] = varslice
		}
		outmtx.Lock()
		go func() {
			defer outmtx.Unlock()
			for _, varslice := range flushvariants {
				cmd.outputFormat.Print(outw, seqname, varslice)
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

func bucketVarsliceByRef(varslice []hgvs.Variant) map[string]map[string]int {
	byref := map[string]map[string]int{}
	for _, v := range varslice {
		if v.Ref == "" && v.New == "" {
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

func printVCF(out io.Writer, seqname string, varslice []hgvs.Variant) {
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
		fmt.Fprintf(out, "%s\t%d\t%s\t%s\t.\t.\t%s\tGT\t0/1\n", seqname, varslice[0].Position, ref, strings.Join(altslice, ","), info)
	}
}

func printPVCF(out io.Writer, seqname string, varslice []hgvs.Variant) {
	for ref, alts := range bucketVarsliceByRef(varslice) {
		altslice := make([]string, 0, len(alts))
		for alt := range alts {
			altslice = append(altslice, alt)
		}
		sort.Strings(altslice)
		for i, a := range altslice {
			alts[a] = i + 1
		}
		fmt.Fprintf(out, "%s\t%d\t%s\t%s\t.\t.\tGT", seqname, varslice[0].Position, ref, strings.Join(altslice, ","))
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
			fmt.Fprintf(out, "\t%d/%d", a1, a2)
		}
		out.Write([]byte{'\n'})
	}
}

func printHGVS(out io.Writer, seqname string, varslice []hgvs.Variant) {
	for i := 0; i < len(varslice)/2; i++ {
		if i > 0 {
			out.Write([]byte{'\t'})
		}
		var1, var2 := varslice[i*2], varslice[i*2+1]
		if var1 == var2 {
			if var1.Ref == var1.New {
				out.Write([]byte{'.'})
			} else {
				fmt.Fprintf(out, "%s:g.%s", seqname, var1.String())
			}
		} else {
			fmt.Fprintf(out, "%s:g.[%s];[%s]", seqname, var1.String(), var2.String())
		}
	}
	out.Write([]byte{'\n'})
}

func printHGVSOneHot(out io.Writer, seqname string, varslice []hgvs.Variant) {
	vars := map[hgvs.Variant]bool{}
	for _, v := range varslice {
		if v.Ref != v.New {
			vars[v] = true
		}
	}

	// sort variants to ensure output is deterministic
	sorted := make([]hgvs.Variant, 0, len(vars))
	for v := range vars {
		sorted = append(sorted, v)
	}
	sort.Slice(sorted, func(a, b int) bool { return hgvs.Less(sorted[a], sorted[b]) })

	for _, v := range sorted {
		fmt.Fprintf(out, "%s.%s", seqname, v.String())
		for i := 0; i < len(varslice); i += 2 {
			if varslice[i] == v || varslice[i+1] == v {
				out.Write([]byte("\t1"))
			} else {
				out.Write([]byte("\t0"))
			}
		}
		out.Write([]byte{'\n'})
	}
}
