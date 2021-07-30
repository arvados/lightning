package lightning

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"

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
	output, err := ioutil.ReadFile(tmpdir + "/out.chr1.tsv")
	if !c.Check(err, check.IsNil) {
		out, _ := exec.Command("find", tmpdir, "-ls").CombinedOutput()
		c.Logf("%s", out)
	}
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr1.1_3delinsGGC	1	0
chr1.41_42delinsAA	1	0
chr1.161A>T	1	0
chr1.178A>T	1	0
chr1.222_224del	1	0
chr1.302_305delinsAAAA	1	0
`))
	output, err = ioutil.ReadFile(tmpdir + "/out.chr2.tsv")
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
	c.Check(string(labels), check.Equals, `0,"input1","out.tsv"
1,"input2","out.tsv"
`)

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + tmpdir,
		"-output-dir=" + tmpdir,
		"-output-format=pvcf",
		"-ref=testdata/ref.fasta",
	}, &buffer, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err = ioutil.ReadFile(tmpdir + "/out.chr1.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	testdata/pipeline1/input1.1.fasta	testdata/pipeline1/input2.1.fasta
chr1	1	.	NNN	GGC	.	.	.	GT	1/1	0/0
chr1	41	.	TT	AA	.	.	.	GT	1/0	0/0
chr1	161	.	A	T	.	.	.	GT	0/1	0/0
chr1	178	.	A	T	.	.	.	GT	0/1	0/0
chr1	221	.	TCCA	T	.	.	.	GT	1/1	0/0
chr1	302	.	TTTT	AAAA	.	.	.	GT	0/1	0/0
`))
	output, err = ioutil.ReadFile(tmpdir + "/out.chr2.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	testdata/pipeline1/input1.1.fasta	testdata/pipeline1/input2.1.fasta
chr2	1	.	TTT	AAA	.	.	.	GT	0/0	0/1
chr2	125	.	CTT	AAA	.	.	.	GT	0/0	1/1
chr2	240	.	ATTTTTCTTGCTCTC	A	.	.	.	GT	1/0	0/0
chr2	258	.	CCTTGTATTTTT	AA	.	.	.	GT	1/0	0/0
chr2	315	.	C	A	.	.	.	GT	1/0	0/0
chr2	469	.	GTGG	G	.	.	.	GT	1/0	0/0
chr2	471	.	GG	AA	.	.	.	GT	0/1	0/0
`))

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
	c.Check(sortLines(string(output)), check.Equals, sortLines(`#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	1	.	NNN	GGC	.	.	AC=2
chr1	41	.	TT	AA	.	.	AC=1
chr1	161	.	A	T	.	.	AC=1
chr1	178	.	A	T	.	.	AC=1
chr1	221	.	TCCA	T	.	.	AC=2
chr1	302	.	TTTT	AAAA	.	.	AC=1
`))
	output, err = ioutil.ReadFile(tmpdir + "/out.chr2.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr2	1	.	TTT	AAA	.	.	AC=1
chr2	125	.	CTT	AAA	.	.	AC=2
chr2	240	.	ATTTTTCTTGCTCTC	A	.	.	AC=1
chr2	258	.	CCTTGTATTTTT	AA	.	.	AC=1
chr2	315	.	C	A	.	.	AC=1
chr2	469	.	GTGG	G	.	.	AC=1
chr2	471	.	GG	AA	.	.	AC=1
`))
}
