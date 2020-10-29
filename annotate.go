package main

import (
	"bufio"
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
	variantHash bool
	maxTileSize int
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
		runner.Args = []string{"annotate", "-local=true", fmt.Sprintf("-variant-hash=%v", cmd.variantHash), "-max-tile-size", strconv.Itoa(cmd.maxTileSize), "-i", *inputFilename, "-o", "/mnt/output/tilevariants.tsv"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/tilevariants.tsv")
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
	bufw := bufio.NewWriter(output)

	err = cmd.exportTileDiffs(bufw, input)
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

func (cmd *annotatecmd) exportTileDiffs(outw io.Writer, librdr io.Reader) error {
	var refs []CompactSequence
	var tiles [][]TileVariant
	var tagset [][]byte
	var taglen int
	err := DecodeLibrary(librdr, func(ent *LibraryEntry) error {
		if len(ent.TagSet) > 0 {
			if tagset != nil {
				return errors.New("error: not implemented: input has multiple tagsets")
			}
			tagset = ent.TagSet
			taglen = len(tagset[0])
			tiles = make([][]TileVariant, len(tagset))
		}
		for _, tv := range ent.TileVariants {
			if tv.Tag >= tagID(len(tiles)) {
				return fmt.Errorf("error: reading tile variant for tag %d but only %d tags were loaded", tv.Tag, len(tiles))
			}
			for len(tiles[tv.Tag]) <= int(tv.Variant) {
				tiles[tv.Tag] = append(tiles[tv.Tag], TileVariant{})
			}
			tiles[tv.Tag][tv.Variant] = tv
		}
		for _, cs := range ent.CompactSequences {
			refs = append(refs, cs)
		}
		return nil
	})
	if err != nil {
		return err
	}
	tag2tagid := make(map[string]tagID, len(tagset))
	for tagid, tagseq := range tagset {
		tag2tagid[string(tagseq)] = tagID(tagid)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	log.Infof("len(refs) %d", len(refs))

	outch := make(chan string, 1)
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

	limiter := make(chan bool, runtime.NumCPU()+1)
	var diffwg sync.WaitGroup
	defer diffwg.Wait()

	for _, refcs := range refs {
		refcs := refcs
		var seqnames []string
		for seqname := range refcs.TileSequences {
			seqnames = append(seqnames, seqname)
		}
		sort.Strings(seqnames)
		for _, seqname := range seqnames {
			seqname := seqname
			var refseq []byte
			// tilestart[123] is the index into refseq
			// where the tile for tag 123 was placed.
			tilestart := map[tagID]int{}
			tileend := map[tagID]int{}
			for _, libref := range refcs.TileSequences[seqname] {
				if libref.Variant < 1 {
					return fmt.Errorf("reference %q seq %q uses variant zero at tag %d", refcs.Name, seqname, libref.Tag)
				}
				seq := tiles[libref.Tag][libref.Variant].Sequence
				if len(seq) < taglen {
					return fmt.Errorf("reference %q seq %q uses tile %d variant %d with sequence len %d < taglen %d", refcs.Name, seqname, libref.Tag, libref.Variant, len(seq), taglen)
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
			for tag, tvs := range tiles {
				tag := tagID(tag)
				refstart, ok := tilestart[tag]
				if !ok {
					// Tag didn't place on this
					// reference sequence. (It
					// might place on the same
					// chromosome in a genome
					// anyway, but we don't output
					// the annotations that would
					// result.)
					continue
				}
				for variant, tv := range tvs {
					variant, tv := variant, tv
					if variant == 0 {
						continue
					}
					if len(tv.Sequence) < taglen {
						return fmt.Errorf("tilevar %d,%d has sequence len %d < taglen %d", tag, variant, len(tv.Sequence), taglen)
					}
					var refpart []byte
					endtag := string(tv.Sequence[len(tv.Sequence)-taglen:])
					if endtagid, ok := tag2tagid[endtag]; !ok {
						// Tile variant doesn't end on a tag, so it can only place at the end of a chromosome.
						refpart = refseq[refstart:]
						log.Warnf("%x tilevar %d,%d endtag not in ref: %s", tv.Blake2b[:13], tag, variant, endtag)
					} else if refendtagstart, ok := tilestart[endtagid]; !ok {
						// Ref ends a chromsome with a (possibly very large) variant of this tile, but genomes with this tile don't.
						// Give up. (TODO: something smarter)
						log.Debugf("%x not annotating tilevar %d,%d because end tag %d is not in ref", tv.Blake2b[:13], tag, variant, endtagid)
						continue
					} else {
						// Non-terminal tile vs. non-terminal reference.
						refpart = refseq[refstart : refendtagstart+taglen]
						log.Tracef("\n%x tilevar %d,%d endtag %s endtagid %d refendtagstart %d", tv.Blake2b[:13], tag, variant, endtag, endtagid, refendtagstart)
					}
					if len(refpart) > cmd.maxTileSize {
						log.Warnf("%x tilevar %d,%d skipping long diff, ref %s seq %s ref len %d", tv.Blake2b[:13], tag, variant, refcs.Name, seqname, len(refpart))
						continue
					}
					if len(tv.Sequence) > cmd.maxTileSize {
						log.Warnf("%x tilevar %d,%d skipping long diff, ref %s seq %s variant len %d", tv.Blake2b[:13], tag, variant, refcs.Name, seqname, len(tv.Sequence))
						continue
					}
					// log.Printf("\n%x @ refstart %d \n< %s\n> %s\n", tv.Blake2b, refstart, refpart, tv.Sequence)

					diffwg.Add(1)
					limiter <- true
					go func() {
						defer func() {
							<-limiter
							diffwg.Done()
						}()
						diffs, _ := hgvs.Diff(strings.ToUpper(string(refpart)), strings.ToUpper(string(tv.Sequence)), 0)
						for _, diff := range diffs {
							diff.Position += refstart
							var varid string
							if cmd.variantHash {
								varid = fmt.Sprintf("%x", tv.Blake2b)[:13]
							} else {
								varid = strconv.Itoa(variant)
							}
							outch <- fmt.Sprintf("%d\t%s\t%s\t%s:g.%s\n", tag, varid, refcs.Name, seqname, diff.String())
						}
					}()
				}
			}
		}
	}
	return nil
}
