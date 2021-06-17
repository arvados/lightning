package lightning

import (
	"bytes"
	"io/ioutil"
	"os"

	"github.com/kshedden/gonpy"
	"gopkg.in/check.v1"
)

type exportNumpySuite struct{}

var _ = check.Suite(&exportNumpySuite{})

func (s *exportNumpySuite) TestFastaToNumpy(c *check.C) {
	tmpdir := c.MkDir()

	err := ioutil.WriteFile(tmpdir+"/chr1-12-100.bed", []byte("chr1\t12\t100\ttest.1\n"), 0644)
	c.Check(err, check.IsNil)

	var buffer bytes.Buffer
	exited := (&importer{}).RunCommand("import", []string{"-local=true", "-o", tmpdir + "/library.gob.gz", "-tag-library", "testdata/tags", "-output-tiles", "-save-incomplete-tiles", "testdata/a.1.fasta", "testdata/tinyref.fasta"}, &bytes.Buffer{}, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)
	exited = (&exportNumpy{}).RunCommand("export-numpy", []string{"-local=true", "-input-dir", tmpdir, "-output-dir", tmpdir, "-output-annotations", tmpdir + "/annotations.csv", "-regions", tmpdir + "/chr1-12-100.bed"}, &buffer, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	f, err := os.Open(tmpdir + "/matrix.npy")
	c.Assert(err, check.IsNil)
	defer f.Close()
	npy, err := gonpy.NewReader(f)
	c.Assert(err, check.IsNil)
	variants, err := npy.GetInt16()
	c.Assert(err, check.IsNil)
	c.Check(variants, check.HasLen, 6)
	for i := 0; i < 4 && i < len(variants); i += 2 {
		if variants[i] == 1 {
			c.Check(variants[i+1], check.Equals, int16(2), check.Commentf("i=%d, v=%v", i, variants))
		} else {
			c.Check(variants[i], check.Equals, int16(2), check.Commentf("i=%d, v=%v", i, variants))
		}
	}
	for i := 4; i < 6 && i < len(variants); i += 2 {
		c.Check(variants[i], check.Equals, int16(1), check.Commentf("i=%d, v=%v", i, variants))
	}
	annotations, err := ioutil.ReadFile(tmpdir + "/annotations.csv")
	c.Check(err, check.IsNil)
	c.Logf("%s", string(annotations))
	c.Check(string(annotations), check.Matches, `(?ms)(.*\n)?1,1,2,chr1:g.84_85insACTGCGATCTGA\n.*`)
	c.Check(string(annotations), check.Matches, `(?ms)(.*\n)?1,1,1,chr1:g.87_96delinsGCATCTGCA\n.*`)
}

func sortUints(variants []int16) {
	for i := 0; i < len(variants); i += 2 {
		if variants[i] > variants[i+1] {
			for j := 0; j < len(variants); j++ {
				variants[j], variants[j+1] = variants[j+1], variants[j]
			}
			return
		}
	}
}

func (s *exportNumpySuite) TestOnehot(c *check.C) {
	for _, trial := range []struct {
		incols  int
		in      []int16
		outcols int
		out     []int16
	}{
		{2, []int16{1, 1, 1, 1}, 2, []int16{1, 1, 1, 1}},
		{2, []int16{1, 1, 1, 2}, 3, []int16{1, 1, 0, 1, 0, 1}},
		{
			// 2nd column => 3 one-hot columns
			// 4th column => 0 one-hot columns
			4, []int16{
				1, 1, 0, 0,
				1, 2, 1, 0,
				1, 3, 0, 0,
			}, 5, []int16{
				1, 1, 0, 0, 0,
				1, 0, 1, 0, 1,
				1, 0, 0, 1, 0,
			},
		},
	} {
		out, _, outcols := recodeOnehot(trial.in, trial.incols)
		c.Check(out, check.DeepEquals, trial.out)
		c.Check(outcols, check.Equals, trial.outcols)
	}
}
