package main

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/lucasb-eyer/go-colorful"
	log "github.com/sirupsen/logrus"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

type importer struct {
	tagLibraryFile string
	refFile        string
	outputFile     string
	projectUUID    string
	runLocal       bool
	skipOOO        bool
	outputTiles    bool
	includeNoCalls bool
	outputStats    string
	encoder        *gob.Encoder
}

func (cmd *importer) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cmd.tagLibraryFile, "tag-library", "", "tag library fasta `file`")
	flags.StringVar(&cmd.refFile, "ref", "", "reference fasta `file`")
	flags.StringVar(&cmd.outputFile, "o", "-", "output `file`")
	flags.StringVar(&cmd.projectUUID, "project", "", "project `UUID` for output data")
	flags.BoolVar(&cmd.runLocal, "local", false, "run on local host (default: run in an arvados container)")
	flags.BoolVar(&cmd.skipOOO, "skip-ooo", false, "skip out-of-order tags")
	flags.BoolVar(&cmd.outputTiles, "output-tiles", false, "include tile variant sequences in output file")
	flags.BoolVar(&cmd.includeNoCalls, "include-no-calls", false, "treat tiles with no-calls as regular tiles")
	flags.StringVar(&cmd.outputStats, "output-stats", "", "output stats to `file` (json)")
	priority := flags.Int("priority", 500, "container request priority")
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	loglevel := flags.String("loglevel", "info", "logging threshold (trace, debug, info, warn, error, fatal, or panic)")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if cmd.tagLibraryFile == "" {
		fmt.Fprintln(os.Stderr, "cannot import without -tag-library argument")
		return 2
	} else if flags.NArg() == 0 {
		flags.Usage()
		return 2
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	lvl, err := log.ParseLevel(*loglevel)
	if err != nil {
		return 2
	}
	log.SetLevel(lvl)

	if !cmd.runLocal {
		runner := arvadosContainerRunner{
			Name:        "lightning import",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: cmd.projectUUID,
			RAM:         60000000000,
			VCPUs:       16,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(&cmd.tagLibraryFile, &cmd.refFile, &cmd.outputFile)
		if err != nil {
			return 1
		}
		inputs := flags.Args()
		for i := range inputs {
			err = runner.TranslatePaths(&inputs[i])
			if err != nil {
				return 1
			}
		}
		if cmd.outputFile == "-" {
			cmd.outputFile = "/mnt/output/library.gob"
		} else {
			// Not yet implemented, but this should write
			// the collection to an existing collection,
			// possibly even an in-place update.
			err = errors.New("cannot specify output file in container mode: not implemented")
			return 1
		}
		runner.Args = append([]string{"import",
			"-local=true",
			"-loglevel=" + *loglevel,
			fmt.Sprintf("-skip-ooo=%v", cmd.skipOOO),
			fmt.Sprintf("-output-tiles=%v", cmd.outputTiles),
			fmt.Sprintf("-include-no-calls=%v", cmd.includeNoCalls),
			"-output-stats", "/mnt/output/stats.json",
			"-tag-library", cmd.tagLibraryFile,
			"-ref", cmd.refFile,
			"-o", cmd.outputFile,
		}, inputs...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/library.gob")
		return 0
	}

	infiles, err := listInputFiles(flags.Args())
	if err != nil {
		return 1
	}

	taglib, err := cmd.loadTagLibrary()
	if err != nil {
		return 1
	}

	var output io.WriteCloser
	if cmd.outputFile == "-" {
		output = nopCloser{stdout}
	} else {
		output, err = os.OpenFile(cmd.outputFile, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer output.Close()
	}
	bufw := bufio.NewWriter(output)
	cmd.encoder = gob.NewEncoder(bufw)

	tilelib := &tileLibrary{taglib: taglib, includeNoCalls: cmd.includeNoCalls, skipOOO: cmd.skipOOO}
	if cmd.outputTiles {
		cmd.encoder.Encode(LibraryEntry{TagSet: taglib.Tags()})
		tilelib.encoder = cmd.encoder
	}
	go func() {
		for range time.Tick(10 * time.Minute) {
			log.Printf("tilelib.Len() == %d", tilelib.Len())
		}
	}()

	err = cmd.tileInputs(tilelib, infiles)
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
	return 0
}

func (cmd *importer) tileFasta(tilelib *tileLibrary, infile string) (tileSeq, []importStats, error) {
	var input io.ReadCloser
	input, err := os.Open(infile)
	if err != nil {
		return nil, nil, err
	}
	defer input.Close()
	if strings.HasSuffix(infile, ".gz") {
		input, err = gzip.NewReader(input)
		if err != nil {
			return nil, nil, err
		}
		defer input.Close()
	}
	return tilelib.TileFasta(infile, input)
}

func (cmd *importer) loadTagLibrary() (*tagLibrary, error) {
	log.Printf("tag library %s load starting", cmd.tagLibraryFile)
	f, err := os.Open(cmd.tagLibraryFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rdr io.ReadCloser = f
	if strings.HasSuffix(cmd.tagLibraryFile, ".gz") {
		rdr, err = gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: gzip: %s", cmd.tagLibraryFile, err)
		}
		defer rdr.Close()
	}
	var taglib tagLibrary
	err = taglib.Load(rdr)
	if err != nil {
		return nil, err
	}
	if taglib.Len() < 1 {
		return nil, fmt.Errorf("cannot tile: tag library is empty")
	}
	log.Printf("tag library %s load done", cmd.tagLibraryFile)
	return &taglib, nil
}

var (
	vcfFilenameRe    = regexp.MustCompile(`\.vcf(\.gz)?$`)
	fasta1FilenameRe = regexp.MustCompile(`\.1\.fa(sta)?(\.gz)?$`)
	fasta2FilenameRe = regexp.MustCompile(`\.2\.fa(sta)?(\.gz)?$`)
	fastaFilenameRe  = regexp.MustCompile(`\.fa(sta)?(\.gz)?$`)
)

func listInputFiles(paths []string) (files []string, err error) {
	for _, path := range paths {
		if fi, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("%s: stat failed: %s", path, err)
		} else if !fi.IsDir() {
			if !fasta2FilenameRe.MatchString(path) {
				files = append(files, path)
			}
			continue
		}
		d, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%s: open failed: %s", path, err)
		}
		defer d.Close()
		names, err := d.Readdirnames(0)
		if err != nil {
			return nil, fmt.Errorf("%s: readdir failed: %s", path, err)
		}
		sort.Strings(names)
		for _, name := range names {
			if vcfFilenameRe.MatchString(name) {
				files = append(files, filepath.Join(path, name))
			} else if fastaFilenameRe.MatchString(name) && !fasta2FilenameRe.MatchString(name) {
				files = append(files, filepath.Join(path, name))
			}
		}
		d.Close()
	}
	for _, file := range files {
		if fastaFilenameRe.MatchString(file) {
			continue
		} else if vcfFilenameRe.MatchString(file) {
			if _, err := os.Stat(file + ".csi"); err == nil {
				continue
			} else if _, err = os.Stat(file + ".tbi"); err == nil {
				continue
			} else {
				return nil, fmt.Errorf("%s: cannot read without .tbi or .csi index file", file)
			}
		} else {
			return nil, fmt.Errorf("don't know how to handle filename %s", file)
		}
	}
	return
}

func (cmd *importer) tileInputs(tilelib *tileLibrary, infiles []string) error {
	starttime := time.Now()
	errs := make(chan error, 1)
	todo := make(chan func() error, len(infiles)*2)
	allstats := make([][]importStats, len(infiles)*2)
	var encodeJobs sync.WaitGroup
	for idx, infile := range infiles {
		idx, infile := idx, infile
		var phases sync.WaitGroup
		phases.Add(2)
		variants := make([][]tileVariantID, 2)
		if fasta1FilenameRe.MatchString(infile) {
			todo <- func() error {
				defer phases.Done()
				log.Printf("%s starting", infile)
				defer log.Printf("%s done", infile)
				tseqs, stats, err := cmd.tileFasta(tilelib, infile)
				allstats[idx*2] = stats
				var kept, dropped int
				variants[0], kept, dropped = tseqs.Variants()
				log.Printf("%s found %d unique tags plus %d repeats", infile, kept, dropped)
				return err
			}
			infile2 := fasta1FilenameRe.ReplaceAllString(infile, `.2.fa$1$2`)
			todo <- func() error {
				defer phases.Done()
				log.Printf("%s starting", infile2)
				defer log.Printf("%s done", infile2)
				tseqs, stats, err := cmd.tileFasta(tilelib, infile2)
				allstats[idx*2+1] = stats
				var kept, dropped int
				variants[1], kept, dropped = tseqs.Variants()
				log.Printf("%s found %d unique tags plus %d repeats", infile2, kept, dropped)

				return err
			}
		} else if fastaFilenameRe.MatchString(infile) {
			todo <- func() error {
				defer phases.Done()
				defer phases.Done()
				log.Printf("%s starting", infile)
				defer log.Printf("%s done", infile)
				tseqs, stats, err := cmd.tileFasta(tilelib, infile)
				allstats[idx*2] = stats
				if err != nil {
					return err
				}
				totlen := 0
				for _, tseq := range tseqs {
					totlen += len(tseq)
				}
				log.Printf("%s tiled %d seqs, total len %d", infile, len(tseqs), totlen)
				return cmd.encoder.Encode(LibraryEntry{
					CompactSequences: []CompactSequence{{Name: infile, TileSequences: tseqs}},
				})
			}
			// Don't write out a CompactGenomes entry
			continue
		} else if vcfFilenameRe.MatchString(infile) {
			for phase := 0; phase < 2; phase++ {
				phase := phase
				todo <- func() error {
					defer phases.Done()
					log.Printf("%s phase %d starting", infile, phase+1)
					defer log.Printf("%s phase %d done", infile, phase+1)
					tseqs, stats, err := cmd.tileGVCF(tilelib, infile, phase)
					allstats[idx*2] = stats
					var kept, dropped int
					variants[phase], kept, dropped = tseqs.Variants()
					log.Printf("%s phase %d found %d unique tags plus %d repeats", infile, phase+1, kept, dropped)
					return err
				}
			}
		} else {
			panic(fmt.Sprintf("bug: unhandled filename %q", infile))
		}
		encodeJobs.Add(1)
		go func() {
			defer encodeJobs.Done()
			phases.Wait()
			if len(errs) > 0 {
				return
			}
			err := cmd.encoder.Encode(LibraryEntry{
				CompactGenomes: []CompactGenome{{Name: infile, Variants: flatten(variants)}},
			})
			if err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}()
	}
	go close(todo)
	var tileJobs sync.WaitGroup
	var running int64
	for i := 0; i < runtime.NumCPU()*9/8+1; i++ {
		tileJobs.Add(1)
		atomic.AddInt64(&running, 1)
		go func() {
			defer tileJobs.Done()
			defer atomic.AddInt64(&running, -1)
			for fn := range todo {
				if len(errs) > 0 {
					return
				}
				err := fn()
				if err != nil {
					select {
					case errs <- err:
					default:
					}
				}
				remain := len(todo) + int(atomic.LoadInt64(&running)) - 1
				if remain < cap(todo) {
					ttl := time.Now().Sub(starttime) * time.Duration(remain) / time.Duration(cap(todo)-remain)
					eta := time.Now().Add(ttl)
					log.Printf("progress %d/%d, eta %v (%v)", cap(todo)-remain, cap(todo), eta, ttl)
				}
			}
		}()
	}
	tileJobs.Wait()
	encodeJobs.Wait()

	go close(errs)
	err := <-errs
	if err != nil {
		return err
	}

	if cmd.outputStats != "" {
		f, err := os.OpenFile(cmd.outputStats, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		var flatstats []importStats
		for _, stats := range allstats {
			flatstats = append(flatstats, stats...)
		}
		err = json.NewEncoder(f).Encode(flatstats)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cmd *importer) tileGVCF(tilelib *tileLibrary, infile string, phase int) (tileseq tileSeq, stats []importStats, err error) {
	if cmd.refFile == "" {
		err = errors.New("cannot import vcf: reference data (-ref) not specified")
		return
	}
	args := []string{"bcftools", "consensus", "--fasta-ref", cmd.refFile, "-H", fmt.Sprint(phase + 1), infile}
	indexsuffix := ".tbi"
	if _, err := os.Stat(infile + ".csi"); err == nil {
		indexsuffix = ".csi"
	}
	if out, err := exec.Command("docker", "image", "ls", "-q", "lightning-runtime").Output(); err == nil && len(out) > 0 {
		args = append([]string{
			"docker", "run", "--rm",
			"--log-driver=none",
			"--volume=" + infile + ":" + infile + ":ro",
			"--volume=" + infile + indexsuffix + ":" + infile + indexsuffix + ":ro",
			"--volume=" + cmd.refFile + ":" + cmd.refFile + ":ro",
			"lightning-runtime",
		}, args...)
	}
	consensus := exec.Command(args[0], args[1:]...)
	consensus.Stderr = os.Stderr
	stdout, err := consensus.StdoutPipe()
	defer stdout.Close()
	if err != nil {
		return
	}
	err = consensus.Start()
	if err != nil {
		return
	}
	defer consensus.Wait()
	tileseq, stats, err = tilelib.TileFasta(fmt.Sprintf("%s phase %d", infile, phase+1), stdout)
	if err != nil {
		return
	}
	err = stdout.Close()
	if err != nil {
		return
	}
	err = consensus.Wait()
	if err != nil {
		err = fmt.Errorf("%s phase %d: bcftools: %s", infile, phase, err)
		return
	}
	return
}

func flatten(variants [][]tileVariantID) []tileVariantID {
	ntags := 0
	for _, v := range variants {
		if ntags < len(v) {
			ntags = len(v)
		}
	}
	flat := make([]tileVariantID, ntags*2)
	for i := 0; i < ntags; i++ {
		for hap := 0; hap < 2; hap++ {
			if i < len(variants[hap]) {
				flat[i*2+hap] = variants[hap][i]
			}
		}
	}
	return flat
}

type importstatsplot struct{}

func (cmd *importstatsplot) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := cmd.Plot(stdin, stdout)
	if err != nil {
		log.Errorf("%s", err)
		return 1
	}
	return 0
}

func (cmd *importstatsplot) Plot(stdin io.Reader, stdout io.Writer) error {
	var stats []importStats
	err := json.NewDecoder(stdin).Decode(&stats)
	if err != nil {
		return err
	}

	p, err := plot.New()
	if err != nil {
		return err
	}
	p.Title.Text = "coverage preserved by import (excl X<0.65)"
	p.X.Label.Text = "input base calls ÷ sequence length"
	p.Y.Label.Text = "output base calls ÷ input base calls"
	p.Add(plotter.NewGrid())

	data := map[string]plotter.XYs{}
	for _, stat := range stats {
		data[stat.InputLabel] = append(data[stat.InputLabel], plotter.XY{
			X: float64(stat.InputCoverage) / float64(stat.InputLength),
			Y: float64(stat.TileCoverage) / float64(stat.InputCoverage),
		})
	}

	labels := []string{}
	for label := range data {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	palette, err := colorful.SoftPalette(len(labels))
	if err != nil {
		return err
	}
	nextInPalette := 0
	for idx, label := range labels {
		s, err := plotter.NewScatter(data[label])
		if err != nil {
			return err
		}
		s.GlyphStyle.Color = palette[idx]
		s.GlyphStyle.Radius = vg.Millimeter / 2
		s.GlyphStyle.Shape = draw.CrossGlyph{}
		nextInPalette += 7
		p.Add(s)
		if false {
			p.Legend.Add(label, s)
		}
	}
	p.X.Min = 0.65
	p.X.Max = 1

	w, err := p.WriterTo(8*vg.Inch, 6*vg.Inch, "svg")
	if err != nil {
		return err
	}
	_, err = w.WriteTo(stdout)
	return err
}
