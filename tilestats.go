// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type tilingStats struct {
}

func (cmd *tilingStats) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
			Name:        "lightning tiling-stats",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         8000000000,
			VCPUs:       2,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir)
		if err != nil {
			return 1
		}
		runner.Args = []string{"tiling-stats", "-local=true",
			"-pprof=:6060",
			"-input-dir=" + *inputDir,
			"-output-dir=/mnt/output",
		}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output)
		return 0
	}

	infiles, err := allFiles(*inputDir, matchGobFile)
	if err != nil {
		return 1
	}
	if len(infiles) == 0 {
		err = fmt.Errorf("no input files found in %s", *inputDir)
		return 1
	}
	sort.Strings(infiles)

	var refseqs []CompactSequence
	var reftiledata = make(map[tileLibRef][]byte, 11000000)
	in0, err := open(infiles[0])
	if err != nil {
		return 1
	}
	defer in0.Close()
	var taglen int
	err = DecodeLibrary(in0, strings.HasSuffix(infiles[0], ".gz"), func(ent *LibraryEntry) error {
		if len(ent.TagSet) > 0 {
			taglen = len(ent.TagSet[0])
		}
		refseqs = append(refseqs, ent.CompactSequences...)
		for _, tv := range ent.TileVariants {
			if tv.Ref {
				reftiledata[tileLibRef{tv.Tag, tv.Variant}] = tv.Sequence
			}
		}
		return nil
	})
	if err != nil {
		return 1
	}
	in0.Close()
	if len(refseqs) == 0 {
		err = fmt.Errorf("%s: reference sequence not found", infiles[0])
		return 1
	}
	if taglen == 0 {
		err = fmt.Errorf("%s: tagset not found", infiles[0])
		return 1
	}

	for _, cseq := range refseqs {
		_, basename := filepath.Split(cseq.Name)
		bedname := fmt.Sprintf("%s/%s.bed", *outputDir, basename)
		log.Infof("writing %s", bedname)
		var f *os.File
		f, err = os.OpenFile(bedname, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer f.Close()
		bufw := bufio.NewWriterSize(f, 1<<24)
		seqnames := make([]string, 0, len(cseq.TileSequences))
		for seqname := range cseq.TileSequences {
			seqnames = append(seqnames, seqname)
		}
		sort.Strings(seqnames)
		// Mark duplicate tags (tags that place more than once
		// on the reference)
		duptag := map[tagID]bool{}
		for _, seqname := range seqnames {
			for _, libref := range cseq.TileSequences[seqname] {
				if dup, seen := duptag[libref.Tag]; seen && !dup {
					duptag[libref.Tag] = true
				} else {
					duptag[libref.Tag] = false
				}
			}
		}
		for _, seqname := range seqnames {
			pos := 0
			for _, libref := range cseq.TileSequences[seqname] {
				if duptag[libref.Tag] {
					continue
				}
				tiledata := reftiledata[libref]
				if len(tiledata) <= taglen {
					err = fmt.Errorf("bogus input data: ref tile libref %v has len %d < taglen %d", libref, len(tiledata), taglen)
					return 1
				}
				score := 1000 * countBases(tiledata) / len(tiledata)
				_, err = fmt.Fprintf(bufw, "%s %d %d %d %d . %d %d\n",
					seqname,
					pos, pos+len(tiledata),
					libref.Tag,
					score,
					pos+taglen, pos+len(tiledata)-taglen)
				if err != nil {
					return 1
				}
				pos += len(tiledata) - taglen
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
	}
	return 0
}
