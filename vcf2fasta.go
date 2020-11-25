package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
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
	"strconv"
	"strings"
	"sync"
	"syscall"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type vcf2fasta struct {
	refFile           string
	genomeFile        string
	mask              bool
	gvcfRegionsPy     string
	gvcfRegionsPyData []byte
	gvcfType          string
	projectUUID       string
	outputDir         string
	runLocal          bool
	vcpus             int
	batchArgs

	stderr io.Writer
}

func (cmd *vcf2fasta) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cmd.refFile, "ref", "", "reference fasta `file`")
	flags.StringVar(&cmd.genomeFile, "genome", "", "reference genome `file`")
	flags.BoolVar(&cmd.mask, "mask", false, "mask uncalled regions (default: output hom ref)")
	flags.StringVar(&cmd.gvcfRegionsPy, "gvcf-regions.py", "https://raw.githubusercontent.com/lijiayong/gvcf_regions/master/gvcf_regions.py", "source of gvcf_regions.py")
	flags.StringVar(&cmd.gvcfType, "gvcf-type", "gatk", "gvcf_type argument to gvcf_regions.py: gatk, complete_genomics, freebayes")
	flags.StringVar(&cmd.projectUUID, "project", "", "project `UUID` for containers and output data")
	flags.StringVar(&cmd.outputDir, "output-dir", "", "output directory")
	flags.IntVar(&cmd.vcpus, "vcpus", 0, "number of VCPUs to request for arvados container (default: 2*number of input files, max 32)")
	flags.BoolVar(&cmd.runLocal, "local", false, "run on local host (default: run in an arvados container)")
	cmd.batchArgs.Flags(flags)
	priority := flags.Int("priority", 500, "container request priority")
	pprof := flags.String("pprof", "", "serve Go profile data at http://`[addr]:port`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	} else if cmd.refFile == "" {
		err = errors.New("reference data (-ref) not specified")
		return 2
	} else if flags.NArg() == 0 {
		flags.Usage()
		return 2
	}
	cmd.stderr = stderr

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if cmd.mask {
		err = cmd.loadRegionsPy()
		if err != nil {
			return 1
		}
	}

	if !cmd.runLocal {
		if cmd.outputDir != "" {
			err = errors.New("cannot specify output dir in non-local mode")
			return 2
		}
		if cmd.vcpus < 1 {
			var infiles []string
			infiles, err = listInputFiles(flags.Args())
			if err != nil {
				return 1
			}
			batchsize := (len(infiles) + cmd.batchArgs.batches - 1) / cmd.batchArgs.batches
			if cmd.vcpus = batchsize * 2; cmd.vcpus > 32 {
				cmd.vcpus = 32
			}
		}
		client := arvados.NewClientFromEnv()
		runner := arvadosContainerRunner{
			Name:        "lightning vcf2fasta",
			Client:      client,
			ProjectUUID: cmd.projectUUID,
			RAM:         2<<30 + int64(cmd.vcpus)<<28,
			VCPUs:       cmd.vcpus,
			Priority:    *priority,
			Mounts: map[string]map[string]interface{}{
				"/gvcf_regions.py": map[string]interface{}{
					"kind":    "text",
					"content": string(cmd.gvcfRegionsPyData),
				},
			},
		}
		err = runner.TranslatePaths(&cmd.refFile, &cmd.genomeFile)
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
		var outputs []string
		outputs, err = cmd.batchArgs.RunBatches(context.Background(), func(ctx context.Context, batch int) (string, error) {
			runner := runner
			if cmd.mask {
				runner.RAM += int64(cmd.vcpus) << 31
			}
			runner.Args = []string{"vcf2fasta",
				"-local=true",
				"-ref", cmd.refFile, fmt.Sprintf("-mask=%v", cmd.mask),
				"-genome", cmd.genomeFile,
				"-gvcf-regions.py", "/gvcf_regions.py",
				"-gvcf-type", cmd.gvcfType,
				"-output-dir", "/mnt/output",
			}
			runner.Args = append(runner.Args, cmd.batchArgs.Args(batch)...)
			runner.Args = append(runner.Args, inputs...)
			log.Printf("batch %d: %v", batch, runner.Args)
			return runner.RunContext(ctx)
		})
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, strings.Join(outputs, " "))
		return 0
	}

	infiles, err := listInputFiles(flags.Args())
	if err != nil {
		return 1
	}
	infiles = cmd.batchArgs.Slice(infiles)

	type job struct {
		vcffile string
		phase   int
	}
	todo := make(chan job)
	go func() {
		for _, infile := range infiles {
			for phase := 1; phase <= 2; phase++ {
				todo <- job{vcffile: infile, phase: phase}
			}
		}
		close(todo)
	}()

	done := make(chan error, runtime.NumCPU()*2)
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range todo {
				if len(done) > 0 {
					// a different worker encountered an error
					return
				}
				err := cmd.vcf2fasta(job.vcffile, job.phase)
				if err != nil {
					done <- fmt.Errorf("%s phase %d: %s", job.vcffile, job.phase, err)
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	err = <-done
	if err != nil {
		return 1
	}
	return 0
}

func maybeInDocker(args, mountfiles []string) []string {
	if out, err := exec.Command("docker", "image", "ls", "-q", "lightning-runtime").Output(); err != nil || len(out) == 0 {
		return args
	}
	dockerrun := []string{
		"docker", "run", "--rm", "-i",
		"--log-driver=none",
	}
	for _, f := range mountfiles {
		dockerrun = append(dockerrun, "--volume="+f+":"+f+":ro")
	}
	dockerrun = append(dockerrun, "lightning-runtime")
	dockerrun = append(dockerrun, args...)
	return dockerrun
}

func (cmd *vcf2fasta) vcf2fasta(infile string, phase int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, basename := filepath.Split(infile)
	outfile := filepath.Join(cmd.outputDir, fmt.Sprintf("%s.%d.fasta.gz", basename, phase))
	outf, err := os.OpenFile(outfile, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("error opening output file: %s", err)
	}
	defer outf.Close()
	bufw := bufio.NewWriterSize(outf, 8*1024*1024)
	gzipw := gzip.NewWriter(bufw)
	defer gzipw.Close()

	var maskfifo string // filename of mask fifo if we're running bedtools, otherwise ""

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	if cmd.mask {
		chrSize := map[string]int{}

		vcffile, err := os.Open(infile)
		if err != nil {
			return err
		}
		defer vcffile.Close()
		var rdr io.Reader = vcffile
		if strings.HasSuffix(infile, ".gz") {
			rdr, err = gzip.NewReader(vcffile)
			if err != nil {
				return err
			}
		}
		contigre := regexp.MustCompile(`([^=,]*)=([^>,]*)`)
		scanner := bufio.NewScanner(rdr)
		for scanner.Scan() {
			if s := scanner.Text(); !strings.HasPrefix(s, "##") {
				break
			} else if !strings.HasPrefix(s, "##contig=<") {
				continue
			} else {
				kv := map[string]string{}
				for _, m := range contigre.FindAllStringSubmatch(s[10:], -1) {
					kv[m[1]] = m[2]
				}
				if kv["ID"] != "" && kv["length"] != "" {
					chrSize[kv["ID"]], _ = strconv.Atoi(kv["length"])
				}
			}
		}
		if err = scanner.Err(); err != nil {
			return fmt.Errorf("error scanning input file %q: %s", infile, err)
		}
		var regions bytes.Buffer
		bedargs := []string{"python2", "-", "--gvcf_type", cmd.gvcfType, infile}
		bed := exec.CommandContext(ctx, bedargs[0], bedargs[1:]...)
		bed.Stdin = bytes.NewBuffer(cmd.gvcfRegionsPyData)
		bed.Stdout = &regions
		bed.Stderr = cmd.stderr
		log.Printf("running %v", bed.Args)
		err = bed.Run()
		log.Printf("exited %v", bed.Args)
		if err != nil {
			return fmt.Errorf("gvcf_regions: %s", err)
		}

		if cmd.genomeFile != "" {
			// Read chromosome sizes from genome file in
			// case any weren't specified in the VCF
			// header.
			genomeFile, err := os.Open(cmd.genomeFile)
			if err != nil {
				return fmt.Errorf("error opening genome file %q: %s", cmd.genomeFile, err)
			}
			scanner := bufio.NewScanner(genomeFile)
			for scanner.Scan() {
				var chr string
				var size int
				_, err := fmt.Sscanf(scanner.Text(), "%s\t%d", &chr, &size)
				if err != nil {
					return fmt.Errorf("error parsing genome file %q: %s", cmd.genomeFile, err)
				}
				if chrSize[chr] == 0 {
					chrSize[chr] = size
				}
			}
			if err = scanner.Err(); err != nil {
				return fmt.Errorf("error scanning genome file %q: %s", cmd.genomeFile, err)
			}
		}

		// "bedtools complement" expects the chromosome sizes
		// ("genome file") to appear in the same order as the
		// chromosomes in the input vcf, so we need to sort
		// them.
		scanner = bufio.NewScanner(bytes.NewBuffer(append([]byte(nil), regions.Bytes()...)))
		var sortedGenomeFile bytes.Buffer
		for scanner.Scan() {
			var chr string
			var size int
			_, err := fmt.Sscanf(scanner.Text(), "%s\t%d", &chr, &size)
			if err != nil {
				return fmt.Errorf("error parsing gvcf_regions output: %s", err)
			}
			if size, ok := chrSize[chr]; ok {
				fmt.Fprintf(&sortedGenomeFile, "%s\t%d\n", chr, size)
				delete(chrSize, chr)
			}
		}

		// The bcftools --mask argument needs to end in ".bed"
		// in order to be parsed as a BED file, so we need to
		// use a named pipe instead of stdin.
		tempdir, err := ioutil.TempDir("", "")
		if err != nil {
			return fmt.Errorf("TempDir: %s", err)
		}
		defer os.RemoveAll(tempdir)
		maskfifo = filepath.Join(tempdir, "fifo.bed")
		err = syscall.Mkfifo(maskfifo, 0600)
		if err != nil {
			return fmt.Errorf("mkfifo: %s", err)
		}

		// bedtools complement can't seem to read from a pipe
		// reliably -- "Error: line number 1 of file
		// /dev/stdin has 1 fields, but 3 were expected." --
		// so we stage to a temp file.
		regionsFile := filepath.Join(tempdir, "gvcf_regions.bed")
		err = ioutil.WriteFile(regionsFile, regions.Bytes(), 0644)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			maskfifow, err := os.OpenFile(maskfifo, os.O_WRONLY, 0)
			if err != nil {
				errs <- err
				return
			}
			defer maskfifow.Close()

			bedcompargs := []string{"bedtools", "complement", "-i", regionsFile, "-g", "/dev/stdin"}
			bedcompargs = maybeInDocker(bedcompargs, []string{cmd.genomeFile})
			bedcomp := exec.CommandContext(ctx, bedcompargs[0], bedcompargs[1:]...)
			bedcomp.Stdin = &sortedGenomeFile
			bedcomp.Stdout = maskfifow
			bedcomp.Stderr = cmd.stderr
			log.Printf("running %v", bedcomp.Args)
			err = bedcomp.Run()
			log.Printf("exited %v", bedcomp.Args)
			if err != nil {
				errs <- fmt.Errorf("bedtools complement: %s", err)
				return
			}
			err = maskfifow.Close()
			if err != nil {
				errs <- err
				return
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		consargs := []string{"bcftools", "consensus", "--fasta-ref", cmd.refFile, "-H", fmt.Sprint(phase)}
		if maskfifo != "" {
			consargs = append(consargs, "--mask", maskfifo)
		}
		consargs = append(consargs, infile)
		indexsuffix := ".tbi"
		if _, err := os.Stat(infile + ".csi"); err == nil {
			indexsuffix = ".csi"
		}
		mounts := []string{infile, infile + indexsuffix, cmd.refFile}
		if maskfifo != "" {
			mounts = append(mounts, maskfifo)
		}
		consargs = maybeInDocker(consargs, mounts)

		consensus := exec.CommandContext(ctx, consargs[0], consargs[1:]...)
		consensus.Stderr = os.Stderr
		consensus.Stdout = gzipw
		consensus.Stderr = cmd.stderr
		log.Printf("running %v", consensus.Args)
		err = consensus.Run()
		if err != nil {
			errs <- fmt.Errorf("bcftools consensus: %s", err)
			return
		}
		log.Printf("exited %v", consensus.Args)
		err = gzipw.Close()
		if err != nil {
			errs <- err
			return
		}
		err = bufw.Flush()
		if err != nil {
			errs <- err
			return
		}
		errs <- outf.Close()
	}()

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			cancel()
			wg.Wait()
			return err
		}
	}
	return nil
}

func (cmd *vcf2fasta) loadRegionsPy() error {
	if strings.HasPrefix(cmd.gvcfRegionsPy, "http") {
		resp, err := http.Get(cmd.gvcfRegionsPy)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("get %q: http status %d", cmd.gvcfRegionsPy, resp.StatusCode)
		}
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("get %q: read body: %s", cmd.gvcfRegionsPy, err)
		}
		cmd.gvcfRegionsPyData = buf
		return nil
	} else {
		buf, err := ioutil.ReadFile(cmd.gvcfRegionsPy)
		if err != nil {
			return err
		}
		cmd.gvcfRegionsPyData = buf
		return nil
	}
}
