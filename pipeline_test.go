package main

import (
	"bytes"
	"fmt"
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
		c.Logf("%s", statsout.String())
	}
}

func (s *pipelineSuite) TestImportMerge(c *check.C) {
	libfile := make([]string, 2)
	tmpdir := c.MkDir()

	var wg sync.WaitGroup
	for i, infile := range []string{
		"testdata/pipeline1/",
		"testdata/ref.fasta",
	} {
		i, infile := i, infile
		c.Logf("TestImportMerge: %s", infile)
		libfile[i] = fmt.Sprintf("%s/%d.gob", tmpdir, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := (&importer{}).RunCommand("lightning import", []string{"-local=true", "-o=" + libfile[i], "-skip-ooo=true", "-output-tiles", "-tag-library", "testdata/tags", infile}, bytes.NewReader(nil), &bytes.Buffer{}, os.Stderr)
			c.Check(code, check.Equals, 0)
		}()
	}
	wg.Wait()

	merged := &bytes.Buffer{}
	code := (&merger{}).RunCommand("lightning merge", []string{"-local", libfile[0], libfile[1]}, bytes.NewReader(nil), merged, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Logf("len(merged) %d", merged.Len())

	statsout := &bytes.Buffer{}
	code = (&stats{}).RunCommand("lightning stats", []string{"-local"}, merged, statsout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(statsout.Len() > 0, check.Equals, true)
	c.Logf("%s", statsout.String())
}
