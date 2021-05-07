package lightning

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

	"github.com/klauspost/pgzip"
	log "github.com/sirupsen/logrus"
)

type importer struct {
	tagLibraryFile      string
	refFile             string
	outputFile          string
	projectUUID         string
	loglevel            string
	priority            int
	runLocal            bool
	skipOOO             bool
	outputTiles         bool
	saveIncompleteTiles bool
	outputStats         string
	matchChromosome     *regexp.Regexp
	encoder             *gob.Encoder
	retainAfterEncoding bool // keep imported genomes/refseqs in memory after writing to disk
	batchArgs
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
	flags.BoolVar(&cmd.saveIncompleteTiles, "save-incomplete-tiles", false, "treat tiles with no-calls as regular tiles")
	flags.StringVar(&cmd.outputStats, "output-stats", "", "output stats to `file` (json)")
	cmd.batchArgs.Flags(flags)
	matchChromosome := flags.String("match-chromosome", "^(chr)?([0-9]+|X|Y|MT?)$", "import chromosomes that match the given `regexp`")
	flags.IntVar(&cmd.priority, "priority", 500, "container request priority")
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	flags.StringVar(&cmd.loglevel, "loglevel", "info", "logging threshold (trace, debug, info, warn, error, fatal, or panic)")
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

	lvl, err := log.ParseLevel(cmd.loglevel)
	if err != nil {
		return 2
	}
	log.SetLevel(lvl)

	cmd.matchChromosome, err = regexp.Compile(*matchChromosome)
	if err != nil {
		return 1
	}

	if !cmd.runLocal {
		err = cmd.runBatches(stdout, flags.Args())
		if err != nil {
			return 1
		}
		return 0
	}

	infiles, err := listInputFiles(flags.Args())
	if err != nil {
		return 1
	}
	infiles = cmd.batchArgs.Slice(infiles)

	taglib, err := cmd.loadTagLibrary()
	if err != nil {
		return 1
	}

	var outw, outf io.WriteCloser
	if cmd.outputFile == "-" {
		outw = nopCloser{stdout}
	} else {
		outf, err = os.OpenFile(cmd.outputFile, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer outf.Close()
		if strings.HasSuffix(cmd.outputFile, ".gz") {
			outw = pgzip.NewWriter(outf)
		} else {
			outw = outf
		}
	}
	bufw := bufio.NewWriterSize(outw, 64*1024*1024)
	cmd.encoder = gob.NewEncoder(bufw)

	tilelib := &tileLibrary{taglib: taglib, retainNoCalls: cmd.saveIncompleteTiles, skipOOO: cmd.skipOOO}
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
	err = outw.Close()
	if err != nil {
		return 1
	}
	if outf != nil && outf != outw {
		err = outf.Close()
		if err != nil {
			return 1
		}
	}
	return 0
}

func (cmd *importer) runBatches(stdout io.Writer, inputs []string) error {
	if cmd.outputFile != "-" {
		// Not yet implemented, but this should write
		// the collection to an existing collection,
		// possibly even an in-place update.
		return errors.New("cannot specify output file in container mode: not implemented")
	}
	runner := arvadosContainerRunner{
		Name:        "lightning import",
		Client:      arvadosClientFromEnv,
		ProjectUUID: cmd.projectUUID,
		APIAccess:   true,
		RAM:         700000000000,
		VCPUs:       96,
		Priority:    cmd.priority,
		KeepCache:   1,
	}
	err := runner.TranslatePaths(&cmd.tagLibraryFile, &cmd.refFile, &cmd.outputFile)
	if err != nil {
		return err
	}
	for i := range inputs {
		err = runner.TranslatePaths(&inputs[i])
		if err != nil {
			return err
		}
	}

	outputs, err := cmd.batchArgs.RunBatches(context.Background(), func(ctx context.Context, batch int) (string, error) {
		runner := runner
		if cmd.batches > 1 {
			runner.Name += fmt.Sprintf(" (batch %d of %d)", batch, cmd.batches)
		}
		runner.Args = []string{"import",
			"-local=true",
			"-loglevel=" + cmd.loglevel,
			"-pprof=:6061",
			fmt.Sprintf("-skip-ooo=%v", cmd.skipOOO),
			fmt.Sprintf("-output-tiles=%v", cmd.outputTiles),
			fmt.Sprintf("-save-incomplete-tiles=%v", cmd.saveIncompleteTiles),
			"-match-chromosome", cmd.matchChromosome.String(),
			"-output-stats", "/mnt/output/stats.json",
			"-tag-library", cmd.tagLibraryFile,
			"-ref", cmd.refFile,
			"-o", "/mnt/output/library.gob.gz",
		}
		runner.Args = append(runner.Args, cmd.batchArgs.Args(batch)...)
		runner.Args = append(runner.Args, inputs...)
		return runner.RunContext(ctx)
	})
	if err != nil {
		return err
	}
	var outfiles []string
	for _, o := range outputs {
		outfiles = append(outfiles, o+"/library.gob.gz")
	}
	fmt.Fprintln(stdout, strings.Join(outfiles, " "))
	return nil
}

func (cmd *importer) tileFasta(tilelib *tileLibrary, infile string) (tileSeq, []importStats, error) {
	var input io.ReadCloser
	input, err := open(infile)
	if err != nil {
		return nil, nil, err
	}
	defer input.Close()
	input = ioutil.NopCloser(bufio.NewReaderSize(input, 8*1024*1024))
	if strings.HasSuffix(infile, ".gz") {
		input, err = pgzip.NewReader(input)
		if err != nil {
			return nil, nil, err
		}
		defer input.Close()
	}
	return tilelib.TileFasta(infile, input, cmd.matchChromosome)
}

func (cmd *importer) loadTagLibrary() (*tagLibrary, error) {
	log.Printf("tag library %s load starting", cmd.tagLibraryFile)
	f, err := open(cmd.tagLibraryFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rdr := ioutil.NopCloser(bufio.NewReaderSize(f, 64*1024*1024))
	if strings.HasSuffix(cmd.tagLibraryFile, ".gz") {
		rdr, err = gzip.NewReader(rdr)
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

				if cmd.retainAfterEncoding {
					tilelib.mtx.Lock()
					if tilelib.refseqs == nil {
						tilelib.refseqs = map[string]map[string][]tileLibRef{}
					}
					tilelib.refseqs[infile] = tseqs
					tilelib.mtx.Unlock()
				}

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
			variants := flatten(variants)
			err := cmd.encoder.Encode(LibraryEntry{
				CompactGenomes: []CompactGenome{{Name: infile, Variants: variants}},
			})
			if err != nil {
				select {
				case errs <- err:
				default:
				}
			}
			if cmd.retainAfterEncoding {
				tilelib.mtx.Lock()
				if tilelib.compactGenomes == nil {
					tilelib.compactGenomes = make(map[string][]tileVariantID)
				}
				tilelib.compactGenomes[infile] = variants
				tilelib.mtx.Unlock()
			}
		}()
	}
	go close(todo)
	var tileJobs sync.WaitGroup
	var running int64
	for i := 0; i < runtime.GOMAXPROCS(-1)*2; i++ {
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
	if len(errs) > 0 {
		// Must not wait on encodeJobs in this case. If the
		// tileJobs goroutines exited early, some funcs in
		// todo haven't been called, so the corresponding
		// encodeJobs will wait forever.
		return <-errs
	}
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
	tileseq, stats, err = tilelib.TileFasta(fmt.Sprintf("%s phase %d", infile, phase+1), stdout, cmd.matchChromosome)
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
