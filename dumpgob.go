package lightning

import (
	"bufio"
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

type dumpGob struct{}

func (cmd *dumpGob) RunCommand(prog string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
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
	inputFilename := flags.String("i", "-", "input `file` (library)")
	outputFilename := flags.String("o", "-", "output `file`")
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
		if *outputFilename != "-" {
			err = errors.New("cannot specify output file in container mode: not implemented")
			return 1
		}
		runner := arvadosContainerRunner{
			Name:        "lightning dumpgob",
			Client:      arvados.NewClientFromEnv(),
			ProjectUUID: *projectUUID,
			RAM:         4000000000,
			VCPUs:       1,
			Priority:    *priority,
		}
		err = runner.TranslatePaths(inputFilename)
		if err != nil {
			return 1
		}
		runner.Args = []string{"dumpgob", "-local=true", fmt.Sprintf("-pprof=%v", *pprof), "-i", *inputFilename, "-o", "/mnt/output/dumpgob.txt"}
		var output string
		output, err = runner.Run()
		if err != nil {
			return 1
		}
		fmt.Fprintln(stdout, output+"/dumpgob.txt")
		return 0
	}

	input, err := open(*inputFilename)
	if err != nil {
		return 1
	}
	defer input.Close()
	output, err := os.OpenFile(*outputFilename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 1
	}
	defer output.Close()
	bufw := bufio.NewWriterSize(output, 8*1024*1024)

	var n, nCG, nCS, nTV int
	err = DecodeLibrary(input, strings.HasSuffix(*inputFilename, ".gz"), func(ent *LibraryEntry) error {
		if n%1000000 == 0 {
			fmt.Fprintf(stderr, "ent %d\n", n)
		}
		n++
		if len(ent.TagSet) > 0 {
			fmt.Fprintf(bufw, "ent %d: TagSet, len %d, taglen %d\n", n, len(ent.TagSet), len(ent.TagSet[0]))
		}
		for _, cg := range ent.CompactGenomes {
			nCG++
			fmt.Fprintf(bufw, "ent %d: CompactGenome, name %q, len(Variants) %d\n", n, cg.Name, len(cg.Variants))
		}
		for _, cs := range ent.CompactSequences {
			nCS++
			fmt.Fprintf(bufw, "ent %d: CompactSequence, name %q, len(TileSequences) %d\n", n, cs.Name, len(cs.TileSequences))
		}
		for _, tv := range ent.TileVariants {
			nTV++
			fmt.Fprintf(bufw, "ent %d: TileVariant, tag %d, variant %d, hash %x, len(seq) %d\n", n, tv.Tag, tv.Variant, tv.Blake2b, len(tv.Sequence))
		}
		return nil
	})
	if err != nil {
		return 1
	}
	fmt.Fprintf(bufw, "total: ents %d, CompactGenomes %d, CompactSequences %d, TileVariants %d\n", n, nCG, nCS, nTV)
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
