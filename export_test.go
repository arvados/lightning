package lightning

import (
	"bytes"
	"io/ioutil"
	"os"

	"gopkg.in/check.v1"
)

type exportSuite struct{}

var _ = check.Suite(&exportSuite{})

func (s *exportSuite) TestFastaToHGVS(c *check.C) {
	tmpdir := c.MkDir()

	err := ioutil.WriteFile(tmpdir+"/chr1-12-100.bed", []byte("chr1\t12\t100\ttest.1\n"), 0644)
	c.Check(err, check.IsNil)

	var buffer bytes.Buffer
	exited := (&importer{}).RunCommand("import", []string{"-local=true", "-tag-library", "testdata/tags", "-output-tiles", "-save-incomplete-tiles", "testdata/pipeline1", "testdata/ref.fasta"}, &bytes.Buffer{}, &buffer, os.Stderr)
	c.Assert(exited, check.Equals, 0)
	ioutil.WriteFile(tmpdir+"/library.gob", buffer.Bytes(), 0644)

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + tmpdir,
		"-output-dir=" + tmpdir,
		"-output-format=hgvs-onehot",
		"-output-labels=" + tmpdir + "/labels.csv",
		"-ref=testdata/ref.fasta",
	}, &buffer, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err := ioutil.ReadFile(tmpdir + "/out.chr1.csv")
	c.Check(err, check.IsNil)
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr1.1_3delinsGGC	1	0
chr1.41_42delinsAA	1	0
chr1.161A>T	1	0
chr1.178A>T	1	0
chr1.222_224del	1	0
chr1.302_305delinsAAAA	1	0
`))
	output, err = ioutil.ReadFile(tmpdir + "/out.chr2.csv")
	c.Check(err, check.IsNil)
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr2.1_3delinsAAA	0	1
chr2.125_127delinsAAA	0	1
chr2.241_254del	1	0
chr2.258_269delinsAA	1	0
chr2.315C>A	1	0
chr2.470_472del	1	0
chr2.471_472delinsAA	1	0
`))
	labels, err := ioutil.ReadFile(tmpdir + "/labels.csv")
	c.Check(err, check.IsNil)
	c.Check(string(labels), check.Equals, `0,"input1","out.csv"
1,"input2","out.csv"
`)

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + tmpdir,
		"-output-dir=" + tmpdir,
		"-output-format=vcf",
		"-ref=testdata/ref.fasta",
	}, &buffer, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err = ioutil.ReadFile(tmpdir + "/out.chr1.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr1	1	NNN	GGC	1/1	0/0
chr1	41	TT	AA	1/0	0/0
chr1	161	A	T	0/1	0/0
chr1	178	A	T	0/1	0/0
chr1	221	TCCA	T	1/1	0/0
chr1	302	TTTT	AAAA	0/1	0/0
`))
	output, err = ioutil.ReadFile(tmpdir + "/out.chr2.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr2	1	TTT	AAA	0/0	0/1
chr2	125	CTT	AAA	0/0	1/1
chr2	240	ATTTTTCTTGCTCTC	A	1/0	0/0
chr2	258	CCTTGTATTTTT	AA	1/0	0/0
chr2	315	C	A	1/0	0/0
chr2	469	GTGG	G	1/0	0/0
chr2	471	GG	AA	0/1	0/0
`))
}
