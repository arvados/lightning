// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bufio"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

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
	pprofdir := flags.String("pprof-dir", "", "write Go profile data to `directory` periodically")
	runlocal := flags.Bool("local", false, "run on local host (default: run in an arvados container)")
	projectUUID := flags.String("project", "", "project `UUID` for output data")
	priority := flags.Int("priority", 500, "container request priority")
	preemptible := flags.Bool("preemptible", true, "request preemptible instance")
	outputDir := flags.String("output-dir", "./out", "output `directory`")
	tagsPerFile := flags.Int("tags-per-file", 50000, "tags per file (nfiles will be ~10M÷x)")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	inputDirs := flags.Args()
	if len(inputDirs) == 0 {
		err = errors.New("no input dirs specified")
		return 2
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}
	if *pprofdir != "" {
		go writeProfilesPeriodically(*pprofdir)
	}

	if !*runlocal {
		runner := arvadosContainerRunner{
			Name:        "lightning slice",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         500000000000,
			VCPUs:       64,
			Priority:    *priority,
			KeepCache:   2,
			APIAccess:   true,
			Preemptible: *preemptible,
		}
		for i := range inputDirs {
			err = runner.TranslatePaths(&inputDirs[i])
			if err != nil {
				return 1
			}
		}
		runner.Args = append([]string{"slice", "-local=true",
			"-pprof", ":6060",
			"-output-dir", "/mnt/output",
		}, inputDirs...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output)
		return 0
	}

	err = Slice(*tagsPerFile, *outputDir, inputDirs)
	if err != nil {
		return 1
	}
	return 0
}

// Read tags+tiles+genomes from srcdir, write to dstdir with (up to)
// the specified number of tags per file.
func Slice(tagsPerFile int, dstdir string, srcdirs []string) error {
	var infiles []string
	for _, srcdir := range srcdirs {
		files, err := allFiles(srcdir, matchGobFile)
		if err != nil {
			return err
		}
		infiles = append(infiles, files...)
	}
	// dirNamespace[dir] is an int in [0,len(dirNamespace)), used below to
	// namespace variant numbers from different dirs.
	dirNamespace := map[string]tileVariantID{}
	for _, path := range infiles {
		dir, _ := filepath.Split(path)
		if _, ok := dirNamespace[dir]; !ok {
			dirNamespace[dir] = tileVariantID(len(dirNamespace))
		}
	}
	namespaces := tileVariantID(len(dirNamespace))

	var (
		tagset     [][]byte
		tagsetOnce sync.Once
		fs         []*os.File
		bufws      []*bufio.Writer
		gzws       []*pgzip.Writer
		encs       []*gob.Encoder

		countTileVariants int64
		countGenomes      int64
		countReferences   int64
	)

	throttle := throttle{Max: runtime.GOMAXPROCS(0)}
	for _, infile := range infiles {
		infile := infile
		throttle.Go(func() error {
			f, err := open(infile)
			if err != nil {
				return err
			}
			defer f.Close()
			dir, _ := filepath.Split(infile)
			namespace := dirNamespace[dir]
			log.Printf("reading %s (namespace %d)", infile, namespace)
			return DecodeLibrary(f, strings.HasSuffix(infile, ".gz"), func(ent *LibraryEntry) error {
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
				atomic.AddInt64(&countTileVariants, int64(len(ent.TileVariants)))
				for _, tv := range ent.TileVariants {
					tv.Variant = tv.Variant*namespaces + namespace
					fileno := 0
					if !tv.Ref {
						fileno = int(tv.Tag) / tagsPerFile
					}
					err := encs[fileno].Encode(LibraryEntry{
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
				atomic.AddInt64(&countGenomes, int64(len(ent.CompactGenomes)))
				for _, cg := range ent.CompactGenomes {
					for i, v := range cg.Variants {
						if v > 0 {
							cg.Variants[i] = v*namespaces + namespace
						}
					}
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
				atomic.AddInt64(&countReferences, int64(len(ent.CompactSequences)))
				if len(ent.CompactSequences) > 0 {
					for _, cs := range ent.CompactSequences {
						for _, tseq := range cs.TileSequences {
							for i, libref := range tseq {
								tseq[i].Variant = libref.Variant*namespaces + namespace
							}
						}
					}
					err := encs[0].Encode(LibraryEntry{CompactSequences: ent.CompactSequences})
					if err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
	throttle.Wait()
	if throttle.Err() != nil {
		closeOutFiles(fs, bufws, gzws, encs)
		return throttle.Err()
	}
	defer log.Printf("Total %d tile variants, %d genomes, %d reference sequences", countTileVariants, countGenomes, countReferences)
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
