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

type exportHGVS struct {
}

func (cmd *exportHGVS) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	outputFilename := flags.String("o", "-", "fasta output `file`")
	pick := flags.String("pick", "", "`name` of single genome to export")
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
			Name:        "lightning export-hgvs",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         128000000000,
			VCPUs:       2,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"export-hgvs", "-local=true", "-pick", *pick, "-ref", *refname, "-i", *inputFilename, "-o", "/mnt/output/export.csv"}
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
	var tilelib tileLibrary
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
		output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer output.Close()
	}
	bufw := bufio.NewWriter(output)
	err = cmd.export(bufw, input, tilelib.taglib.keylen, refseq, cgs)
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

func (cmd *exportHGVS) export(out io.Writer, librdr io.Reader, taglen int, refseq map[string][]tileLibRef, cgs []CompactGenome) error {
	need := map[tileLibRef]bool{}
	var seqnames []string
	for seqname, librefs := range refseq {
		seqnames = append(seqnames, seqname)
		for _, libref := range librefs {
			need[libref] = true
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

	refpos := 0
	for _, seqname := range seqnames {
		variantAt := map[int][]hgvs.Variant{} // variantAt[chromOffset][genomeIndex*2+phase]
		for refstep, libref := range refseq[seqname] {
			reftile := tileVariant[libref]
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
					// If needed, extend the
					// reference sequence up to
					// the tag at the end of the
					// genometile sequence.
					refstepend := refstep + 1
					for refstepend < len(refseq[seqname]) && len(refSequence) >= taglen && !bytes.EqualFold(refSequence[len(refSequence)-taglen:], genometile.Sequence[len(genometile.Sequence)-taglen:]) {
						if &refSequence[0] == &reftile.Sequence[0] {
							refSequence = append([]byte(nil), refSequence...)
						}
						refSequence = append(refSequence, tileVariant[refseq[seqname][refstepend]].Sequence...)
						refstepend++
					}
					vars, _ := hgvs.Diff(strings.ToUpper(string(refSequence)), strings.ToUpper(string(genometile.Sequence)), time.Second)
					for _, v := range vars {
						v.Position += refpos
						log.Debugf("%s seq %s phase %d tag %d tile diff %s\n", cg.Name, seqname, phase, libref.Tag, v.String())
						varslice := variantAt[v.Position]
						if varslice == nil {
							varslice = make([]hgvs.Variant, len(cgs)*2)
						}
						varslice[cgidx*2+phase] = v
						variantAt[v.Position] = varslice
					}
				}
			}
			refpos += len(reftile.Sequence) - taglen

			// Flush entries from variantAt that are
			// behind refpos. Flush all entries if this is
			// the last reftile of the path/chromosome.
			var flushpos []int
			lastrefstep := refstep == len(refseq[seqname])-1
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
				for i := 0; i < len(cgs); i++ {
					if i > 0 {
						out.Write([]byte{'\t'})
					}
					var1, var2 := varslice[i*2], varslice[i*2+1]
					if var1.Position == 0 && var2.Position == 0 {
						out.Write([]byte{'.'})
					} else if var1 == var2 {
						fmt.Fprintf(out, "%s:g.%s", seqname, var1.String())
					} else {
						if var1.Position == 0 {
							var1.Position = pos
						}
						if var2.Position == 0 {
							var2.Position = pos
						}
						fmt.Fprintf(out, "%s:g.[%s];[%s]", seqname, var1.String(), var2.String())
					}
				}
				out.Write([]byte{'\n'})
			}
		}
	}
	return nil
}
