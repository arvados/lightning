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
	log "github.com/sirupsen/logrus"
)

type anno2vcf struct {
}

func (cmd *anno2vcf) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	inputDir := flags.String("input-dir", "./in", "input `directory`")
	outputDir := flags.String("output-dir", "./out", "output `directory`")
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
		runner := arvadosContainerRunner{
			Name:        "lightning anno2vcf",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         500000000000,
			VCPUs:       64,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir)
		if err != nil {
			return 1
		}
		runner.Args = []string{"anno2vcf", "-local=true",
			"-pprof", ":6060",
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

	d, err := open(*inputDir)
	if err != nil {
		log.Print(err)
		return 1
	}
	defer d.Close()
	fis, err := d.Readdir(-1)
	if err != nil {
		log.Print(err)
		return 1
	}
	d.Close()
	sort.Slice(fis, func(i, j int) bool { return fis[i].Name() < fis[j].Name() })

	type call struct {
		tile      int
		variant   int
		position  int
		deletion  []byte
		insertion []byte
	}
	allcalls := map[string][]*call{}
	var mtx sync.Mutex
	thr := throttle{Max: runtime.GOMAXPROCS(0)}
	log.Print("reading input files")
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), "annotations.csv") {
			continue
		}
		filename := *inputDir + "/" + fi.Name()
		thr.Go(func() error {
			log.Printf("reading %s", filename)
			buf, err := ioutil.ReadFile(filename)
			if err != nil {
				return fmt.Errorf("%s: %s", filename, err)
			}
			lines := bytes.Split(buf, []byte{'\n'})
			calls := map[string][]*call{}
			for lineIdx, line := range lines {
				if len(line) == 0 {
					continue
				}
				if lineIdx & ^0xfff == 0 && thr.Err() != nil {
					return nil
				}
				fields := bytes.Split(line, []byte{','})
				if len(fields) < 8 {
					return fmt.Errorf("%s line %d: wrong number of fields (%d < %d): %q", fi.Name(), lineIdx+1, len(fields), 8, line)
				}
				tile, _ := strconv.ParseInt(string(fields[0]), 10, 64)
				variant, _ := strconv.ParseInt(string(fields[2]), 10, 64)
				position, _ := strconv.ParseInt(string(fields[5]), 10, 64)
				seq := string(fields[4])
				if calls[seq] == nil {
					calls[seq] = make([]*call, 0, len(lines)/50)
				}
				del := fields[6]
				ins := fields[7]
				if (len(del) == 0 || len(ins) == 0) && len(fields) >= 9 {
					// "123,,AA,T" means 123insAA
					// preceded by T. We record it
					// here as "122 T TAA" to
					// avoid writing an empty
					// "ref" field in our
					// VCF. Similarly, we record
					// deletions as "122 TAA T"
					// rather than "123 AA .".
					del = append(append(make([]byte, 0, len(fields[8])+len(del)), fields[8]...), del...)
					ins = append(append(make([]byte, 0, len(fields[8])+len(ins)), fields[8]...), ins...)
					position -= int64(len(fields[8]))
				} else {
					del = append([]byte(nil), del...)
					ins = append([]byte(nil), ins...)
				}
				calls[seq] = append(calls[seq], &call{
					tile:      int(tile),
					variant:   int(variant),
					position:  int(position),
					deletion:  del,
					insertion: ins,
				})
			}
			mtx.Lock()
			for seq, seqcalls := range calls {
				allcalls[seq] = append(allcalls[seq], seqcalls...)
			}
			mtx.Unlock()
			return nil
		})
	}
	err = thr.Wait()
	if err != nil {
		return 1
	}
	thr = throttle{Max: len(allcalls)}
	for seq, seqcalls := range allcalls {
		seq, seqcalls := seq, seqcalls
		thr.Go(func() error {
			log.Printf("%s: sorting", seq)
			sort.Slice(seqcalls, func(i, j int) bool {
				ii, jj := seqcalls[i], seqcalls[j]
				if cmp := ii.position - jj.position; cmp != 0 {
					return cmp < 0
				}
				if cmp := len(ii.deletion) - len(jj.deletion); cmp != 0 {
					return cmp < 0
				}
				if cmp := bytes.Compare(ii.insertion, jj.insertion); cmp != 0 {
					return cmp < 0
				}
				if cmp := ii.tile - jj.tile; cmp != 0 {
					return cmp < 0
				}
				return ii.variant < jj.variant
			})

			vcfFilename := fmt.Sprintf("%s/annotations.%s.vcf", *outputDir, seq)
			log.Printf("%s: writing %s", seq, vcfFilename)

			f, err := os.Create(vcfFilename)
			if err != nil {
				return err
			}
			defer f.Close()
			bufw := bufio.NewWriterSize(f, 1<<20)
			_, err = fmt.Fprintf(bufw, `##fileformat=VCFv4.0
##INFO=<ID=TV,Number=.,Type=String,Description="tile-variant">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
`)
			if err != nil {
				return err
			}
			placeholder := []byte{'.'}
			for i := 0; i < len(seqcalls); {
				call := seqcalls[i]
				i++
				info := fmt.Sprintf("TV=,%d-%d,", call.tile, call.variant)
				for i < len(seqcalls) &&
					call.position == seqcalls[i].position &&
					len(call.deletion) == len(seqcalls[i].deletion) &&
					bytes.Equal(call.insertion, seqcalls[i].insertion) {
					call = seqcalls[i]
					i++
					info += fmt.Sprintf("%d-%d,", call.tile, call.variant)
				}
				deletion := call.deletion
				if len(deletion) == 0 {
					deletion = placeholder
				}
				insertion := call.insertion
				if len(insertion) == 0 {
					insertion = placeholder
				}
				_, err = fmt.Fprintf(bufw, "%s\t%d\t.\t%s\t%s\t.\t.\t%s\n", seq, call.position, deletion, insertion, info)
				if err != nil {
					return err
				}
			}
			err = bufw.Flush()
			if err != nil {
				return err
			}
			err = f.Close()
			if err != nil {
				return err
			}
			log.Printf("%s: done", seq)
			return nil
		})
	}
	err = thr.Wait()
	if err != nil {
		return 1
	}
	return 0
}
