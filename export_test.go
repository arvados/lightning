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
	var output bytes.Buffer
	exited = (&exporter{}).RunCommand("export", []string{
		"-local=true",
		"-i=" + tmpdir + "/library.gob",
		"-output-format=hgvs-onehot",
		"-output-labels=" + tmpdir + "/labels.csv",
		"-ref=testdata/ref.fasta",
	}, &buffer, &output, os.Stderr)
	c.Check(exited, check.Equals, 0)
	c.Check(sortLines(output.String()), check.Equals, sortLines(`chr1.1_3delinsGGC	1	0
chr1.41_42delinsAA	1	0
chr1.161A>T	1	0
chr1.178A>T	1	0
chr1.222_224del	1	0
chr1.302_305delinsAAAA	1	0
chr2.1_3delinsAAA	0	1
chr2.125_127delinsAAA	0	1
chr2.241_254del	1	0
chr2.258_269delinsAA	1	0
chr2.315C>A	1	0
chr2.470_472del	1	0
chr2.471_472delinsAA	1	0
`))
	labels, err := ioutil.ReadFile(tmpdir + "/labels.csv")
	c.Check(err, check.IsNil)
	c.Check(string(labels), check.Equals, `0,"input1","-"
1,"input2","-"
`)
}
