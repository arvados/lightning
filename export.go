package main

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
	"sort"
	"strings"
	"sync"
	"time"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/arvados/lightning/hgvs"
	log "github.com/sirupsen/logrus"
)

type outputFormat struct {
	Print   func(out io.Writer, seqname string, varslice []hgvs.Variant)
	PadLeft bool
}

var (
	outputFormats = map[string]outputFormat{
		"hgvs": outputFormatHGVS,
		"vcf":  outputFormatVCF,
	}
	outputFormatHGVS = outputFormat{Print: printHGVS}
	outputFormatVCF  = outputFormat{Print: printVCF, PadLeft: true}
)

type exporter struct {
	outputFormat outputFormat
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
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	refname := flags.String("ref", "", "reference genome `name`")
	inputFilename := flags.String("i", "-", "input `file` (library)")
	outputFilename := flags.String("o", "-", "output `file`")
	outputFormatStr := flags.String("output-format", "hgvs", "output `format`: hgvs or vcf")
	outputBed := flags.String("output-bed", "", "also output bed `file`")
	pick := flags.String("pick", "", "`name` of single genome to export")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
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

	if !*runlocal {
		if *outputFilename != "-" {
			err = errors.New("cannot specify output file in container mode: not implemented")
			return 1
		}
		runner := arvadosContainerRunner{
			Name:        "lightning export",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         240000000000,
			VCPUs:       32,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
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
		runner.Args = []string{"export", "-local=true", "-pick", *pick, "-ref", *refname, "-output-format", *outputFormatStr, "-output-bed", *outputBed, "-i", *inputFilename, "-o", "/mnt/output/export.csv"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/export.csv")
		return 0
	}

	input, err := os.Open(*inputFilename)
	if err != nil {
		return 1
	}
	defer input.Close()

	// Error out early if seeking doesn't work on the input file.
	_, err = input.Seek(0, io.SeekEnd)
	if err != nil {
		return 1
	}
	_, err = input.Seek(0, io.SeekStart)
	if err != nil {
		return 1
	}

	var mtx sync.Mutex
	var cgs []CompactGenome
	tilelib := tileLibrary{
		includeNoCalls: true,
	}
	err = tilelib.LoadGob(context.Background(), input, func(cg CompactGenome) {
		if *pick != "" && *pick != cg.Name {
			return
		}
		log.Debugf("export: pick %q", cg.Name)
		mtx.Lock()
		defer mtx.Unlock()
		cgs = append(cgs, cg)
	})
	if err != nil {
		return 1
	}
	sort.Slice(cgs, func(i, j int) bool { return cgs[i].Name < cgs[j].Name })
	log.Printf("export: pick %q => %d genomes", *pick, len(cgs))

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

	_, err = input.Seek(0, io.SeekStart)
	if err != nil {
		return 1
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
	bufw := bufio.NewWriter(output)

	var bedout *os.File
	var bedbufw *bufio.Writer
	if *outputBed != "" {
		bedout, err = os.OpenFile(*outputBed, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
		defer bedout.Close()
		bedbufw = bufio.NewWriter(bedout)
	}

	err = cmd.export(bufw, bedout, input, tilelib.taglib.keylen, refseq, cgs)
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
	if bedout != nil {
		err = bedbufw.Flush()
		if err != nil {
			return 1
		}
		err = bedout.Close()
		if err != nil {
			return 1
		}
	}
	err = input.Close()
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *exporter) export(out, bedout io.Writer, librdr io.Reader, taglen int, refseq map[string][]tileLibRef, cgs []CompactGenome) error {
	need := map[tileLibRef]bool{}
	var seqnames []string
	for seqname, librefs := range refseq {
		seqnames = append(seqnames, seqname)
		for _, libref := range librefs {
			if libref.Variant != 0 {
				need[libref] = true
			}
		}
	}
	sort.Strings(seqnames)

	for _, cg := range cgs {
		for i, variant := range cg.Variants {
			if variant == 0 {
				continue
			}
			libref := tileLibRef{Tag: tagID(i / 2), Variant: variant}
			need[libref] = true
		}
	}

	log.Infof("export: loading %d tile variants", len(need))
	tileVariant := map[tileLibRef]TileVariant{}
	err := DecodeLibrary(librdr, func(ent *LibraryEntry) error {
		for _, tv := range ent.TileVariants {
			libref := tileLibRef{Tag: tv.Tag, Variant: tv.Variant}
			if need[libref] {
				tileVariant[libref] = tv
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Infof("export: loaded %d tile variants", len(tileVariant))
	var missing []tileLibRef
	for libref := range need {
		if _, ok := tileVariant[libref]; !ok {
			missing = append(missing, libref)
		}
	}
	if len(missing) > 0 {
		if limit := 100; len(missing) > limit {
			log.Warnf("first %d missing tiles: %v", limit, missing[:limit])
		} else {
			log.Warnf("missing tiles: %v", missing)
		}
		return fmt.Errorf("%d needed tiles are missing from library", len(missing))
	}

	log.Infof("assembling %d sequences concurrently", len(seqnames))
	var wg sync.WaitGroup
	outbuf := make([]bytes.Buffer, len(seqnames))
	bedbuf := make([]bytes.Buffer, len(seqnames))
	for seqidx, seqname := range seqnames {
		seqname := seqname
		outbuf := &outbuf[seqidx]
		bedbuf := &bedbuf[seqidx]
		if bedout == nil {
			bedbuf = nil
		}
		// TODO: limit number of goroutines and unflushed bufs to ncpus
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd.exportSeq(outbuf, bedbuf, taglen, seqname, refseq[seqname], tileVariant, cgs)
			log.Infof("assembled %q to outbuf %d bedbuf %d", seqname, outbuf.Len(), bedbuf.Len())
		}()
	}
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i, seqname := range seqnames {
			log.Infof("writing outbuf %s", seqname)
			io.Copy(out, &outbuf[i])
		}
	}()
	if bedout != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, seqname := range seqnames {
				log.Infof("writing bedbuf %s", seqname)
				io.Copy(bedout, &bedbuf[i])
			}
		}()
	}
	wg.Wait()
	return nil
}

// Align genome tiles to reference tiles, write diffs to outw, and (if
// bedw is not nil) write tile coverage to bedw.
func (cmd *exporter) exportSeq(outw, bedw io.Writer, taglen int, seqname string, reftiles []tileLibRef, tileVariant map[tileLibRef]TileVariant, cgs []CompactGenome) {
	refpos := 0
	variantAt := map[int][]hgvs.Variant{} // variantAt[chromOffset][genomeIndex*2+phase]
	for refstep, libref := range reftiles {
		reftile := tileVariant[libref]
		coverage := int64(0) // number of ref bases that are called in genomes -- max is len(reftile.Sequence)*len(cgs)*2
		for cgidx, cg := range cgs {
			for phase := 0; phase < 2; phase++ {
				if len(cg.Variants) <= int(libref.Tag)*2+phase {
					continue
				}
				variant := cg.Variants[int(libref.Tag)*2+phase]
				if variant == 0 {
					continue
				}
				genometile := tileVariant[tileLibRef{Tag: libref.Tag, Variant: variant}]
				if variant == libref.Variant {
					continue
				}
				refSequence := reftile.Sequence
				// If needed, extend the reference
				// sequence up to the tag at the end
				// of the genometile sequence.
				refstepend := refstep + 1
				for refstepend < len(reftiles) && len(refSequence) >= taglen && !bytes.EqualFold(refSequence[len(refSequence)-taglen:], genometile.Sequence[len(genometile.Sequence)-taglen:]) {
					if &refSequence[0] == &reftile.Sequence[0] {
						refSequence = append([]byte(nil), refSequence...)
					}
					refSequence = append(refSequence, tileVariant[reftiles[refstepend]].Sequence...)
					refstepend++
				}
				// (TODO: handle no-calls)
				vars, _ := hgvs.Diff(strings.ToUpper(string(refSequence)), strings.ToUpper(string(genometile.Sequence)), time.Second)
				for _, v := range vars {
					if cmd.outputFormat.PadLeft {
						v = v.PadLeft()
					}
					v.Position += refpos
					log.Debugf("%s seq %s phase %d tag %d tile diff %s\n", cg.Name, seqname, phase, libref.Tag, v.String())
					varslice := variantAt[v.Position]
					if varslice == nil {
						varslice = make([]hgvs.Variant, len(cgs)*2)
						variantAt[v.Position] = varslice
					}
					varslice[cgidx*2+phase] = v
				}
				coverage += int64(len(reftile.Sequence))
			}
		}
		refpos += len(reftile.Sequence) - taglen

		// Flush entries from variantAt that are behind
		// refpos. Flush all entries if this is the last
		// reftile of the path/chromosome.
		var flushpos []int
		lastrefstep := refstep == len(reftiles)-1
		for pos := range variantAt {
			if lastrefstep || pos <= refpos {
				flushpos = append(flushpos, pos)
			}
		}
		sort.Slice(flushpos, func(i, j int) bool { return flushpos[i] < flushpos[j] })
		for _, pos := range flushpos {
			varslice := variantAt[pos]
			delete(variantAt, pos)
			for i := range varslice {
				if varslice[i].Position == 0 {
					varslice[i].Position = pos
				}
			}
			cmd.outputFormat.Print(outw, seqname, varslice)
		}
		if bedw != nil && len(reftile.Sequence) > 0 {
			tilestart := refpos - len(reftile.Sequence) + taglen
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
			score := 1000 * coverage / int64(len(reftile.Sequence)) / int64(len(cgs)) / 2
			fmt.Fprintf(bedw, "%s %d %d %d %d . %d %d\n",
				seqname, tilestart, tileend,
				libref.Tag,
				score,
				thickstart, thickend)
		}
	}
}

func printVCF(out io.Writer, seqname string, varslice []hgvs.Variant) {
	refs := map[string]map[string]int{}
	for _, v := range varslice {
		if v.Ref == "" && v.New == "" {
			continue
		}
		alts := refs[v.Ref]
		if alts == nil {
			alts = map[string]int{}
			refs[v.Ref] = alts
		}
		alts[v.New] = 0
	}
	for ref, alts := range refs {
		var altslice []string
		for alt := range alts {
			altslice = append(altslice, alt)
		}
		sort.Strings(altslice)
		for i, a := range altslice {
			alts[a] = i + 1
		}
		fmt.Fprintf(out, "%s\t%d\t%s\t%s", seqname, varslice[0].Position, ref, strings.Join(altslice, ","))
		for i := 0; i < len(varslice); i += 2 {
			v1, v2 := varslice[i], varslice[i+1]
			a1, a2 := alts[v1.New], alts[v2.New]
			if v1.Ref != ref {
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
