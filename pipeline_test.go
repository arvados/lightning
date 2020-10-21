package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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
	code = (&stats{}).RunCommand("lightning stats", []string{"-local"}, bytes.NewReader(merged.Bytes()), statsout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(statsout.Len() > 0, check.Equals, true)
	c.Logf("%s", statsout.String())

	c.Check(ioutil.WriteFile(tmpdir+"/merged.gob", merged.Bytes(), 0666), check.IsNil)

	hgvsout := &bytes.Buffer{}
	code = (&exporter{}).RunCommand("lightning export", []string{"-local", "-ref", "testdata/ref.fasta", "-output-format", "hgvs", "-i", tmpdir + "/merged.gob"}, bytes.NewReader(nil), hgvsout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(hgvsout.Len() > 0, check.Equals, true)
	c.Logf("%s", hgvsout.String())
	c.Check(hgvsout.String(), check.Equals, `chr1:g.[41_42delinsAA];[41=]
chr1:g.[161=];[161A>T]
chr1:g.[178=];[178A>T]
chr1:g.222_224del
chr1:g.[302=];[302_305delinsAAAA]
chr2:g.[813_826del];[813=]
chr2:g.[830_841delinsAA];[830=]
chr2:g.[887C>A];[887=]
chr2:g.[1042_1044del];[1042=]
chr2:g.[1043=];[1043_1044delinsAA]
`)

	vcfout := &bytes.Buffer{}
	code = (&exporter{}).RunCommand("lightning export", []string{"-local", "-ref", "testdata/ref.fasta", "-output-format", "vcf", "-i", tmpdir + "/merged.gob", "-output-bed", tmpdir + "/export.bed"}, bytes.NewReader(nil), vcfout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(vcfout.Len() > 0, check.Equals, true)
	c.Logf("%s", vcfout.String())
	c.Check(vcfout.String(), check.Equals, `chr1	41	TT	AA	1/0
chr1	161	A	T	0/1
chr1	178	A	T	0/1
chr1	221	TCCA	T	1/1
chr1	302	TTTT	AAAA	0/1
chr2	812	ATTTTTCTTGCTCTC	A	1/0
chr2	830	CCTTGTATTTTT	AA	1/0
chr2	887	C	A	1/0
chr2	1041	GTGG	G	1/0
chr2	1043	GG	AA	0/1
`)
	bedout, err := ioutil.ReadFile(tmpdir + "/export.bed")
	c.Check(err, check.IsNil)
	c.Logf("%s", string(bedout))
	c.Check(string(bedout), check.Equals, `chr1 0 248 0 1000 . 0 224
chr1 224 372 1 500 . 248 348
chr1 348 496 2 0 . 372 472
chr1 472 572 3 0 . 496 572
chr2 572 820 4 500 . 0 796
chr2 796 944 5 0 . 820 920
chr2 920 1068 6 1000 . 944 1044
chr2 1044 1144 7 0 . 1068 1144
`)
}
