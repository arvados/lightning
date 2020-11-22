package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"git.arvados.org/arvados.git/sdk/go/arvados"
	log "github.com/sirupsen/logrus"
)

type ref2genome struct {
	refFile        string
	projectUUID    string
	outputFilename string
	runLocal       bool
}

func (cmd *ref2genome) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}()
	flags := flag.NewFlagSet("", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cmd.refFile, "ref", "", "reference fasta `file`")
	flags.StringVar(&cmd.projectUUID, "project", "", "project `UUID` for containers and output data")
	flags.StringVar(&cmd.outputFilename, "o", "", "output filename")
	flags.BoolVar(&cmd.runLocal, "local", false, "run on local host (default: run in an arvados container)")
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
	}

	if *pprof != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	if !cmd.runLocal {
		if cmd.outputFilename != "" {
			err = errors.New("cannot specify output filename in non-local mode")
			return 2
		}
		runner := arvadosContainerRunner{
			Name:        "lightning ref2genome",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: cmd.projectUUID,
			RAM:         1 << 30,
			Priority:    *priority,
			VCPUs:       1,
		}
		err = runner.TranslatePaths(&cmd.refFile)
		if err != nil {
			return 1
		}
		runner.Args = []string{"ref2genome", "-local=true", "-ref", cmd.refFile, "-o", "/mnt/output/ref.genome"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/ref.genome")
		return 0
	}

	var out io.WriteCloser
	if cmd.outputFilename == "" {
		out = nopCloser{stdout}
	} else {
		out, err = os.OpenFile(cmd.outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			return 1
		}
	}
	f, err := os.Open(cmd.refFile)
	if err != nil {
		return 1
	}
	defer f.Close()
	var in io.Reader
	if strings.HasSuffix(cmd.refFile, ".gz") {
		in, err = gzip.NewReader(f)
		if err != nil {
			return 1
		}
	} else {
		in = f
	}
	label, seqlen := "", 0
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) > 0 && buf[0] == '>' {
			if label != "" {
				fmt.Fprintf(out, "%s\t%d\n", label, seqlen)
			}
			label = strings.TrimSpace(string(buf[1:]))
			label = strings.SplitN(label, " ", 2)[0]
			seqlen = 0
		} else {
			seqlen += len(bytes.TrimSpace(buf))
		}
	}
	if label != "" {
		fmt.Fprintf(out, "%s\t%d\n", label, seqlen)
	}
	if err = scanner.Err(); err != nil {
		return 1
	}
	if err = out.Close(); err != nil {
		return 1
	}
	return 0
}
