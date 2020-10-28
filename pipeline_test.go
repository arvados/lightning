package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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
			code := (&statscmd{}).RunCommand("lightning stats", []string{"-local"}, statsin, statsout, os.Stderr)
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
		"testdata/ref.fasta",
		"testdata/pipeline1/",
	} {
		i, infile := i, infile
		c.Logf("TestImportMerge: %s", infile)
		libfile[i] = fmt.Sprintf("%s/%d.gob", tmpdir, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := []string{"-local=true", "-o=" + libfile[i], "-skip-ooo=true", "-output-tiles", "-tag-library", "testdata/tags"}
			if i == 0 {
				// ref only
				args = append(args, "-include-no-calls")
			}
			args = append(args, infile)
			code := (&importer{}).RunCommand("lightning import", args, bytes.NewReader(nil), &bytes.Buffer{}, os.Stderr)
			c.Check(code, check.Equals, 0)
		}()
	}
	wg.Wait()

	merged := &bytes.Buffer{}
	code := (&merger{}).RunCommand("lightning merge", []string{"-local", libfile[0], libfile[1]}, bytes.NewReader(nil), merged, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Logf("len(merged) %d", merged.Len())

	statsout := &bytes.Buffer{}
	code = (&statscmd{}).RunCommand("lightning stats", []string{"-local"}, bytes.NewReader(merged.Bytes()), statsout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(statsout.Len() > 0, check.Equals, true)
	c.Logf("%s", statsout.String())

	c.Check(ioutil.WriteFile(tmpdir+"/merged.gob", merged.Bytes(), 0666), check.IsNil)

	hgvsout := &bytes.Buffer{}
	code = (&exporter{}).RunCommand("lightning export", []string{"-local", "-ref", "testdata/ref.fasta", "-output-format", "hgvs", "-i", tmpdir + "/merged.gob"}, bytes.NewReader(nil), hgvsout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(hgvsout.Len() > 0, check.Equals, true)
	c.Logf("%s", hgvsout.String())
	c.Check(hgvsout.String(), check.Equals, `chr1:g.1_3delinsGGC	.
chr1:g.[41_42delinsAA];[41=]	.
chr1:g.[161=];[161A>T]	.
chr1:g.[178=];[178A>T]	.
chr1:g.222_224del	.
chr1:g.[302=];[302_305delinsAAAA]	.
.	chr2:g.[1=];[1_3delinsAAA]
.	chr2:g.125_127delinsAAA
chr2:g.[241_254del];[241=]	.
chr2:g.[258_269delinsAA];[258=]	.
chr2:g.[315C>A];[315=]	.
chr2:g.[470_472del];[470=]	.
chr2:g.[471=];[471_472delinsAA]	.
`)

	vcfout := &bytes.Buffer{}
	code = (&exporter{}).RunCommand("lightning export", []string{"-local", "-ref", "testdata/ref.fasta", "-output-format", "vcf", "-i", tmpdir + "/merged.gob", "-output-bed", tmpdir + "/export.bed"}, bytes.NewReader(nil), vcfout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(vcfout.Len() > 0, check.Equals, true)
	c.Logf("%s", vcfout.String())
	c.Check(vcfout.String(), check.Equals, `chr1	1	NNN	GGC	1/1	0/0
chr1	41	TT	AA	1/0	0/0
chr1	161	A	T	0/1	0/0
chr1	178	A	T	0/1	0/0
chr1	221	TCCA	T	1/1	0/0
chr1	302	TTTT	AAAA	0/1	0/0
chr2	1	TTT	AAA	0/0	0/1
chr2	125	CTT	AAA	0/0	1/1
chr2	240	ATTTTTCTTGCTCTC	A	1/0	0/0
chr2	258	CCTTGTATTTTT	AA	1/0	0/0
chr2	315	C	A	1/0	0/0
chr2	469	GTGG	G	1/0	0/0
chr2	471	GG	AA	0/1	0/0
`)
	bedout, err := ioutil.ReadFile(tmpdir + "/export.bed")
	c.Check(err, check.IsNil)
	c.Logf("%s", string(bedout))
	c.Check(string(bedout), check.Equals, `chr1 0 248 0 500 . 0 224
chr1 224 372 1 1000 . 248 348
chr1 348 496 2 1000 . 372 472
chr1 472 572 3 1000 . 496 572
chr2 0 248 4 1000 . 0 224
chr2 224 372 5 750 . 248 348
chr2 348 496 6 1000 . 372 472
chr2 472 572 7 1000 . 496 572
`)

	annotateout := &bytes.Buffer{}
	code = (&annotatecmd{}).RunCommand("lightning annotate", []string{"-local", "-variant-hash=true", "-i", tmpdir + "/merged.gob"}, bytes.NewReader(nil), annotateout, os.Stderr)
	c.Check(code, check.Equals, 0)
	c.Check(annotateout.Len() > 0, check.Equals, true)
	sorted := sortLines(annotateout.String())
	c.Logf("%s", sorted)
	c.Check(sorted, check.Equals, sortLines(`0	8d4fe9a63921b	testdata/ref.fasta	chr1:g.161A>T
0	8d4fe9a63921b	testdata/ref.fasta	chr1:g.178A>T
0	8d4fe9a63921b	testdata/ref.fasta	chr1:g.1_3delinsGGC
0	8d4fe9a63921b	testdata/ref.fasta	chr1:g.222_224del
0	ba4263ca4199c	testdata/ref.fasta	chr1:g.1_3delinsGGC
0	ba4263ca4199c	testdata/ref.fasta	chr1:g.222_224del
0	ba4263ca4199c	testdata/ref.fasta	chr1:g.41_42delinsAA
1	139890345dbb8	testdata/ref.fasta	chr1:g.302_305delinsAAAA
4	cbfca15d241d3	testdata/ref.fasta	chr2:g.125_127delinsAAA
4	cbfca15d241d3	testdata/ref.fasta	chr2:g.1_3delinsAAA
4	f5fafe9450b02	testdata/ref.fasta	chr2:g.241_245delinsAAAAA
4	f5fafe9450b02	testdata/ref.fasta	chr2:g.291C>A
4	fe9a71a42adb4	testdata/ref.fasta	chr2:g.125_127delinsAAA
6	e36dce85efbef	testdata/ref.fasta	chr2:g.471_472delinsAA
6	f81388b184f4a	testdata/ref.fasta	chr2:g.470_472del
`))
}

func sortLines(txt string) string {
	lines := strings.Split(strings.TrimRightFunc(txt, func(c rune) bool { return c == '\n' }), "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}
