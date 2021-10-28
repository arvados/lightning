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
	sort.Slice(fis, func(i, j int) { return fis[i].Name() < fis[j].Name() })

	type call struct {
		tile      int
		variant   int
		sequence  []byte
		position  int
		deletion  []byte
		insertion []byte
	}
	var allcalls []*call
	var mtx sync.Mutex
	throttle := throttle{Max: runtime.GOMAXPROCS(0)}
	log.Print("reading input files")
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), "annotations.csv") {
			continue
		}
		filename := *inputDir + "/" + fi.Name()
		throttle.Acquire()
		go func() {
			defer throttle.Release()
			log.Printf("reading %s", filename)
			buf, err := ioutil.ReadFile(filename)
			if err != nil {
				throttle.Report(fmt.Errorf("%s: %s", filename, err))
				return
			}
			lines := bytes.Split(buf, []byte{'\n'})
			calls := make([]*call, 0, len(lines))
			for lineIdx, line := range lines {
				if len(line) == 0 {
					continue
				}
				if lineIdx & ^0xfff == 0 && throttle.Err() != nil {
					return
				}
				fields := bytes.Split(line, []byte{','})
				if len(fields) != 8 {
					throttle.Report(fmt.Errorf("%s line %d: wrong number of fields (%d != %d): %q", fi.Name(), lineIdx+1, len(fields), 8, line))
					return
				}
				tile, _ := strconv.ParseInt(string(fields[0]), 10, 64)
				variant, _ := strconv.ParseInt(string(fields[2]), 10, 64)
				position, _ := strconv.ParseInt(string(fields[5]), 10, 64)
				calls = append(calls, &call{
					tile:      int(tile),
					variant:   int(variant),
					sequence:  append([]byte(nil), fields[4]...),
					position:  int(position),
					deletion:  append([]byte(nil), fields[6]...),
					insertion: append([]byte(nil), fields[7]...),
				})
			}
			mtx.Lock()
			allcalls = append(allcalls, calls...)
			mtx.Unlock()
		}()
	}
	throttle.Wait()
	if throttle.Err() != nil {
		log.Print(throttle.Err())
		return 1
	}
	log.Print("sorting")
	sort.Slice(allcalls, func(i, j int) bool {
		ii, jj := allcalls[i], allcalls[j]
		if cmp := bytes.Compare(ii.sequence, jj.sequence); cmp != 0 {
			return cmp < 0
		}
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

	vcfFilename := *outputDir + "/annotations.vcf"
	log.Printf("writing %s", vcfFilename)
	f, err := os.Create(vcfFilename)
	if err != nil {
		return 1
	}
	defer f.Close()
	bufw := bufio.NewWriterSize(f, 1<<20)
	_, err = fmt.Fprintf(bufw, `##fileformat=VCFv4.0
##INFO=<ID=TV,Number=.,Type=String,Description="tile-variant">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
`)
	if err != nil {
		return 1
	}
	placeholder := []byte{'.'}
	for i := 0; i < len(allcalls); {
		call := allcalls[i]
		i++
		info := fmt.Sprintf("TV=,%d-%d,", call.tile, call.variant)
		for i < len(allcalls) &&
			bytes.Equal(call.sequence, allcalls[i].sequence) &&
			call.position == allcalls[i].position &&
			len(call.deletion) == len(allcalls[i].deletion) &&
			bytes.Equal(call.insertion, allcalls[i].insertion) {
			call = allcalls[i]
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
		_, err = fmt.Fprintf(bufw, "%s\t%d\t.\t%s\t%s\t.\t.\t%s\n", call.sequence, call.position, deletion, insertion, info)
		if err != nil {
			return 1
		}
	}
	err = bufw.Flush()
	if err != nil {
		return 1
	}
	err = f.Close()
	if err != nil {
		return 1
	}
	return 0
}
