// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/kshedden/gonpy"
	"gopkg.in/check.v1"
)

type exportSuite struct{}

var _ = check.Suite(&exportSuite{})

func (s *exportSuite) TestFastaToHGVS(c *check.C) {
	tmpdir := c.MkDir()

	err := ioutil.WriteFile(tmpdir+"/chr1-12-100.bed", []byte("chr1\t12\t100\ttest.1\n"), 0644)
	c.Check(err, check.IsNil)

	exited := (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-save-incomplete-tiles",
		"-o", tmpdir + "/library1.gob",
		"testdata/ref.fasta",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	exited = (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		// "-save-incomplete-tiles",
		"-o", tmpdir + "/library2.gob",
		"testdata/pipeline1",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	exited = (&merger{}).RunCommand("merge", []string{
		"-local=true",
		"-o", tmpdir + "/library.gob",
		tmpdir + "/library1.gob",
		tmpdir + "/library2.gob",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	input := tmpdir + "/library.gob"

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + tmpdir,
		"-output-format=hgvs-onehot",
		"-output-labels=" + tmpdir + "/labels.csv",
		"-ref=testdata/ref.fasta",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err := ioutil.ReadFile(tmpdir + "/out.chr1.tsv")
	if !c.Check(err, check.IsNil) {
		out, _ := exec.Command("find", tmpdir, "-ls").CombinedOutput()
		c.Logf("%s", out)
	}
	c.Check(sortLines(string(output)), check.Equals, sortLines(`chr1.1_3delinsGGC	1	0
chr1.41T>A	1	0
chr1.42T>A	1	0
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
chr2.469_471del	1	0
chr2.471G>A	1	0
chr2.472G>A	1	0
`))
	labels, err := ioutil.ReadFile(tmpdir + "/labels.csv")
	c.Check(err, check.IsNil)
	c.Check(string(labels), check.Equals, `0,"input1","out.tsv"
1,"input2","out.tsv"
`)

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + tmpdir,
		"-output-format=pvcf",
		"-ref=testdata/ref.fasta",
	}, os.Stderr, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err = ioutil.ReadFile(tmpdir + "/out.chr1.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	testdata/pipeline1/input1.1.fasta	testdata/pipeline1/input2.1.fasta
chr1	1	.	NNN	GGC	.	.	.	GT	1/1	0/0
chr1	41	.	T	A	.	.	.	GT	1/0	0/0
chr1	42	.	T	A	.	.	.	GT	1/0	0/0
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
chr2	468	.	CGTG	C	.	.	.	GT	1/0	0/0
chr2	471	.	G	A	.	.	.	GT	0/1	0/0
chr2	472	.	G	A	.	.	.	GT	0/1	0/0
`))

	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + tmpdir,
		"-output-format=vcf",
		"-ref=testdata/ref.fasta",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	output, err = ioutil.ReadFile(tmpdir + "/out.chr1.vcf")
	c.Check(err, check.IsNil)
	c.Log(string(output))
	c.Check(sortLines(string(output)), check.Equals, sortLines(`#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
chr1	1	.	NNN	GGC	.	.	AC=2
chr1	41	.	T	A	.	.	AC=1
chr1	42	.	T	A	.	.	AC=1
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
chr2	468	.	CGTG	C	.	.	AC=1
chr2	471	.	G	A	.	.	AC=1
chr2	472	.	G	A	.	.	AC=1
`))

	c.Logf("export hgvs-numpy")
	outdir := c.MkDir()
	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + outdir,
		"-output-format=hgvs-numpy",
		"-ref=testdata/ref.fasta",
		"-match-genome=input[12]",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)

	f, err := os.Open(outdir + "/matrix.chr1.npy")
	c.Assert(err, check.IsNil)
	defer f.Close()
	npy, err := gonpy.NewReader(f)
	c.Assert(err, check.IsNil)
	variants, err := npy.GetInt8()
	c.Assert(err, check.IsNil)
	c.Check(variants, check.HasLen, 7*2*2) // 7 variants * 2 alleles * 2 genomes
	c.Check(variants, check.DeepEquals, []int8{
		1, 0, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, // input1.1.fasta
		-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, // input2.1.fasta
	})

	f, err = os.Open(outdir + "/matrix.chr2.npy")
	c.Assert(err, check.IsNil)
	defer f.Close()
	npy, err = gonpy.NewReader(f)
	c.Assert(err, check.IsNil)
	variants, err = npy.GetInt8()
	c.Assert(err, check.IsNil)
	c.Check(variants, check.HasLen, 8*2*2) // 8 variants * 2 alleles * 2 genomes
	c.Check(variants, check.DeepEquals, []int8{
		0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, // input1.1.fasta
		0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // input2.1.fasta
	})

	annotations, err := ioutil.ReadFile(outdir + "/annotations.chr1.csv")
	c.Check(err, check.IsNil)
	c.Logf("%s", string(annotations))
	c.Check(string(annotations), check.Equals, `0,"chr1.1_3delinsGGC"
1,"chr1.41T>A"
2,"chr1.42T>A"
3,"chr1.161A>T"
4,"chr1.178A>T"
5,"chr1.222_224del"
6,"chr1.302_305delinsAAAA"
`)
	annotations, err = ioutil.ReadFile(outdir + "/annotations.chr2.csv")
	c.Check(err, check.IsNil)
	c.Check(string(annotations), check.Equals, `0,"chr2.1_3delinsAAA"
1,"chr2.125_127delinsAAA"
2,"chr2.241_254del"
3,"chr2.258_269delinsAA"
4,"chr2.315C>A"
5,"chr2.469_471del"
6,"chr2.471G>A"
7,"chr2.472G>A"
`)

	c.Logf("export hgvs-numpy with p-value threshold")
	outdir = c.MkDir()
	err = ioutil.WriteFile(tmpdir+"/cases", []byte("input1\n"), 0777)
	c.Assert(err, check.IsNil)
	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-input-dir=" + input,
		"-p-value=0.05",
		"-cases=" + tmpdir + "/cases",
		"-output-dir=" + outdir,
		"-output-format=hgvs-numpy",
		"-ref=testdata/ref.fasta",
		"-match-genome=input[12]",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)

}
