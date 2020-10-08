package main

import (
	"bytes"
	"io"
	"os"
	"sync"

	"gopkg.in/check.v1"
)

type pipelineSuite struct{}

var _ = check.Suite(&pipelineSuite{})

func (s *pipelineSuite) TestImport(c *check.C) {
	for _, infile := range []string{
		"testdata/pipeline1/",
		"testdata/ref.fasta",
	} {
		c.Logf("TestImport: %s", infile)
		var wg sync.WaitGroup

		statsin, importout := io.Pipe()
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := (&importer{}).RunCommand("lightning import", []string{"-local=true", "-skip-ooo=true", "-output-tiles", "-tag-library", "testdata/tags", infile}, bytes.NewReader(nil), importout, os.Stderr)
			c.Check(code, check.Equals, 0)
			importout.Close()
		}()
		statsout := &bytes.Buffer{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := (&stats{}).RunCommand("lightning stats", []string{"-local"}, statsin, statsout, os.Stderr)
			c.Check(code, check.Equals, 0)
		}()
		wg.Wait()
		os.Stdout.Write(statsout.Bytes())
	}
}
