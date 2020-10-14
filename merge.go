package main

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type merger struct {
	stdin   io.Reader
	inputs  []string
	output  io.WriteCloser
	tagSet  [][]byte
	tilelib *tileLibrary
	mapped  map[string]map[tileLibRef]tileVariantID
	mtxTags sync.Mutex
	errs    chan error
}

func (cmd *merger) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	outputFilename := flags.String("o", "-", "output `file`")
	err = flags.Parse(args)
	if err == flag.ErrHelp {
		err = nil
		return 0
	} else if err != nil {
		return 2
	}
	cmd.stdin = stdin
	cmd.inputs = flags.Args()

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
			Name:        "lightning filter",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         64000000000,
			VCPUs:       2,
			Priority:    *priority,
		}
		for i := range cmd.inputs {
			err = runner.TranslatePaths(&cmd.inputs[i])
			if err != nil {
				return 1
			}
		}
		runner.Args = append([]string{"merge", "-local=true",
			"-o", "/mnt/output/library.gob",
		}, cmd.inputs...)
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/library.gob")
		return 0
	}

	if *outputFilename == "-" {
		cmd.output = nopCloser{stdout}
	} else {
		cmd.output, err = os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return 1
		}
		defer cmd.output.Close()
	}

	err = cmd.doMerge()
	if err != nil {
		return 1
	}
	err = cmd.output.Close()
	if err != nil {
		return 1
	}
	return 0
}

func (cmd *merger) setError(err error) {
	select {
	case cmd.errs <- err:
	default:
	}
}

func (cmd *merger) doMerge() error {
	w := bufio.NewWriter(cmd.output)
	encoder := gob.NewEncoder(w)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd.errs = make(chan error, 1)
	cmd.tilelib = &tileLibrary{
		encoder:        encoder,
		includeNoCalls: true,
	}

	cmd.mapped = map[string]map[tileLibRef]tileVariantID{}
	for _, input := range cmd.inputs {
		cmd.mapped[input] = map[tileLibRef]tileVariantID{}
	}

	var wg sync.WaitGroup
	for _, input := range cmd.inputs {
		var infile io.ReadCloser
		if input == "-" {
			infile = ioutil.NopCloser(cmd.stdin)
		} else {
			var err error
			infile, err = os.Open(input)
			if err != nil {
				return err
			}
			defer infile.Close()
		}
		wg.Add(1)
		go func(input string) {
			defer wg.Done()
			log.Printf("%s: reading", input)
			err := cmd.tilelib.LoadGob(ctx, infile, nil)
			if err != nil {
				cmd.setError(fmt.Errorf("%s: load failed: %w", input, err))
				cancel()
				return
			}
			err = infile.Close()
			if err != nil {
				cmd.setError(fmt.Errorf("%s: error closing input file: %w", input, err))
				cancel()
				return
			}
			log.Printf("%s: done", input)
		}(input)
	}
	wg.Wait()
	go close(cmd.errs)
	if err := <-cmd.errs; err != nil {
		return err
	}
	log.Print("flushing")
	err := w.Flush()
	if err != nil {
		return err
	}
	return nil
}
