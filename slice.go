// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	"github.com/klauspost/pgzip"
	log "github.com/sirupsen/logrus"
)

type slicecmd struct{}

func (cmd *slicecmd) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	tagsPerFile := flags.Int("tags-per-file", 50000, "tags per file (nfiles will be ~10M÷x)")
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
			Name:        "lightning slice",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         200000000000,
			VCPUs:       32,
			Priority:    *priority,
			KeepCache:   50,
			APIAccess:   true,
		}
		err = runner.TranslatePaths(inputDir)
		if err != nil {
			return 1
		}
		runner.Args = []string{"slice", "-local=true",
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

	err = Slice(*outputDir, *inputDir, *tagsPerFile)
	if err != nil {
		return 1
	}
	return 0
}

// Read tags+tiles+genomes from srcdir, write to dstdir with (up to)
// the specified number of tags per file.
func Slice(dstdir, srcdir string, tagsPerFile int) error {
	infiles, err := allGobFiles(srcdir)
	if err != nil {
		return err
	}

	var (
		tagset     [][]byte
		tagsetOnce sync.Once
		fs         []*os.File
		bufws      []*bufio.Writer
		gzws       []*pgzip.Writer
		encs       []*gob.Encoder
	)

	throttle := throttle{Max: runtime.GOMAXPROCS(0)}
	for _, path := range infiles {
		path := path
		throttle.Acquire()
		go func() {
			defer throttle.Release()
			f, err := open(path)
			if err != nil {
				throttle.Report(err)
				return
			}
			defer f.Close()
			log.Printf("reading %s", path)
			err = DecodeLibrary(f, strings.HasSuffix(path, ".gz"), func(ent *LibraryEntry) error {
				if err := throttle.Err(); err != nil {
					return err
				}
				if len(ent.TagSet) > 0 {
					tagsetOnce.Do(func() {
						tagset = ent.TagSet
						var err error
						fs, bufws, gzws, encs, err = openOutFiles(dstdir, len(ent.TagSet), tagsPerFile)
						if err != nil {
							throttle.Report(err)
							return
						}
						for _, enc := range encs {
							err = enc.Encode(LibraryEntry{TagSet: tagset})
							if err != nil {
								throttle.Report(err)
								return
							}
						}
					})
				}
				if err := throttle.Err(); err != nil {
					return err
				}
				for _, tv := range ent.TileVariants {
					err := encs[int(tv.Tag)/tagsPerFile].Encode(LibraryEntry{
						TileVariants: []TileVariant{tv},
					})
					if err != nil {
						return err
					}
				}
				// Here, each output file gets a
				// CompactGenome entry for each
				// genome, even if there are no
				// variants in the relevant range.
				// Easier for downstream code.
				for _, cg := range ent.CompactGenomes {
					for i, enc := range encs {
						start := i * tagsPerFile
						end := start + tagsPerFile
						if max := len(cg.Variants)/2 + int(cg.StartTag); end > max {
							end = max
						}
						if start < int(cg.StartTag) {
							start = int(cg.StartTag)
						}
						var variants []tileVariantID
						if start < end {
							variants = cg.Variants[(start-int(cg.StartTag))*2 : (end-int(cg.StartTag))*2]
						}
						err := enc.Encode(LibraryEntry{CompactGenomes: []CompactGenome{{
							Name:     cg.Name,
							Variants: variants,
							StartTag: tagID(start),
							EndTag:   tagID(start + tagsPerFile),
						}}})
						if err != nil {
							return err
						}
					}
				}
				// Write all ref seqs to the first
				// slice. Easier for downstream code.
				if len(ent.CompactSequences) > 0 {
					err := encs[0].Encode(LibraryEntry{CompactSequences: ent.CompactSequences})
					if err != nil {
						return err
					}
				}
				return nil
			})
			throttle.Report(err)
		}()
	}
	throttle.Wait()
	if throttle.Err() != nil {
		closeOutFiles(fs, bufws, gzws, encs)
		return throttle.Err()
	}
	return closeOutFiles(fs, bufws, gzws, encs)
}

func openOutFiles(dstdir string, tags, tagsPerFile int) (fs []*os.File, bufws []*bufio.Writer, gzws []*pgzip.Writer, encs []*gob.Encoder, err error) {
	nfiles := (tags + tagsPerFile - 1) / tagsPerFile
	fs = make([]*os.File, nfiles)
	bufws = make([]*bufio.Writer, nfiles)
	gzws = make([]*pgzip.Writer, nfiles)
	encs = make([]*gob.Encoder, nfiles)
	for i := 0; i*tagsPerFile < tags; i++ {
		fs[i], err = os.Create(dstdir + fmt.Sprintf("/library%04d.gob.gz", i))
		if err != nil {
			return
		}
		bufws[i] = bufio.NewWriterSize(fs[i], 1<<26)
		gzws[i] = pgzip.NewWriter(bufws[i])
		encs[i] = gob.NewEncoder(gzws[i])
	}
	return
}

func closeOutFiles(fs []*os.File, bufws []*bufio.Writer, gzws []*pgzip.Writer, encs []*gob.Encoder) error {
	var firstErr error
	for _, gzw := range gzws {
		if gzw != nil {
			err := gzw.Close()
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, bufw := range bufws {
		if bufw != nil {
			err := bufw.Flush()
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for _, f := range fs {
		if f != nil {
			err := f.Close()
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
