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

type sliceSuite struct{}

var _ = check.Suite(&sliceSuite{})

func (s *sliceSuite) TestImportAndSlice(c *check.C) {
	tmpdir := c.MkDir()
	err := os.Mkdir(tmpdir+"/lib1", 0777)
	c.Assert(err, check.IsNil)
	err = os.Mkdir(tmpdir+"/lib2", 0777)
	c.Assert(err, check.IsNil)
	err = os.Mkdir(tmpdir+"/lib3", 0777)
	c.Assert(err, check.IsNil)
	cwd, err := os.Getwd()
	c.Assert(err, check.IsNil)
	err = os.Symlink(cwd+"/testdata/pipeline1", tmpdir+"/pipeline1")
	c.Assert(err, check.IsNil)
	err = os.Symlink(cwd+"/testdata/pipeline1", tmpdir+"/pipeline1dup")
	c.Assert(err, check.IsNil)

	err = ioutil.WriteFile(tmpdir+"/chr1-12-100.bed", []byte("chr1\t12\t100\ttest.1\n"), 0644)
	c.Check(err, check.IsNil)

	c.Log("=== import testdata/ref ===")
	exited := (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-save-incomplete-tiles",
		"-o", tmpdir + "/lib1/library1.gob",
		"testdata/ref.fasta",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	c.Log("=== import testdata/pipeline1 ===")
	exited = (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-o", tmpdir + "/lib2/library2.gob",
		tmpdir + "/pipeline1",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	c.Log("=== import pipeline1dup ===")
	exited = (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-o", tmpdir + "/lib3/library3.gob",
		tmpdir + "/pipeline1dup",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	slicedir := c.MkDir()

	c.Log("=== slice ===")
	exited = (&slicecmd{}).RunCommand("slice", []string{
		"-local=true",
		"-output-dir=" + slicedir,
		"-tags-per-file=2",
		tmpdir + "/lib1",
		tmpdir + "/lib2",
		tmpdir + "/lib3",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	out, _ := exec.Command("find", slicedir, "-ls").CombinedOutput()
	c.Logf("%s", out)

	c.Log("=== slice-numpy ===")
	{
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		f, err := os.Open(npydir + "/matrix.0000.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err := gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{4, 4})
		variants, err := npy.GetInt16()
		c.Check(variants, check.DeepEquals, []int16{2, 1, 1, 2, -1, -1, 1, 1, 2, 1, 1, 2, -1, -1, 1, 1})

		annotations, err := ioutil.ReadFile(npydir + "/matrix.0000.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", annotations)
		for _, s := range []string{
			"chr1:g.161A>T",
			"chr1:g.178A>T",
			"chr1:g.1_3delinsGGC",
			"chr1:g.222_224del",
		} {
			c.Check(string(annotations), check.Matches, "(?ms).*"+s+".*")
		}

		annotations, err = ioutil.ReadFile(npydir + "/matrix.0002.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", annotations)
		for _, s := range []string{
			",2,chr2:g.1_3delinsAAA",
			",2,chr2:g.125_127delinsAAA",
			",4,chr2:g.125_127delinsAAA",
		} {
			c.Check(string(annotations), check.Matches, "(?ms).*"+s+".*")
		}
	}

	c.Log("=== slice-numpy + regions ===")
	{
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-regions=" + tmpdir + "/chr1-12-100.bed",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
			"-chunked-hgvs-matrix=true",
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		f, err := os.Open(npydir + "/matrix.0000.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err := gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{4, 2})
		variants, err := npy.GetInt16()
		c.Check(variants, check.DeepEquals, []int16{2, 1, -1, -1, 2, 1, -1, -1})

		annotations, err := ioutil.ReadFile(npydir + "/matrix.0000.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", annotations)
		for _, s := range []string{
			"chr1:g.161A>T",
			"chr1:g.178A>T",
			"chr1:g.1_3delinsGGC",
			"chr1:g.222_224del",
		} {
			c.Check(string(annotations), check.Matches, "(?ms).*"+s+".*")
		}

		for _, fnm := range []string{
			npydir + "/matrix.0001.annotations.csv",
			npydir + "/matrix.0002.annotations.csv",
		} {
			annotations, err := ioutil.ReadFile(fnm)
			c.Assert(err, check.IsNil)
			c.Check(string(annotations), check.Equals, "", check.Commentf(fnm))
		}
	}

	err = ioutil.WriteFile(tmpdir+"/chr1and2-100-200.bed", []byte("chr1\t100\t200\ttest.1\nchr2\t100\t200\ttest.2\n"), 0644)
	c.Check(err, check.IsNil)

	c.Log("=== slice-numpy + regions + merge ===")
	{
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-regions=" + tmpdir + "/chr1and2-100-200.bed",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
			"-merge-output=true",
			"-single-hgvs-matrix=true",
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		f, err := os.Open(npydir + "/matrix.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err := gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{4, 4})
		variants, err := npy.GetInt16()
		if c.Check(err, check.IsNil) {
			c.Check(variants, check.DeepEquals, []int16{2, 1, 3, 1, -1, -1, 4, 2, 2, 1, 3, 1, -1, -1, 4, 2})
		}

		annotations, err := ioutil.ReadFile(npydir + "/matrix.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", annotations)
		for _, s := range []string{
			"0,0,1,chr1:g.161A>T",
			"0,0,1,chr1:g.178A>T",
			"4,1,2,chr2:g.125_127delinsAAA",
		} {
			c.Check(string(annotations), check.Matches, "(?ms).*"+s+".*")
		}
	}

	c.Log("=== slice-numpy + chunked hgvs matrix ===")
	{
		err = ioutil.WriteFile(tmpdir+"/casecontrol.tsv", []byte(`SampleID	CC
pipeline1/input1	1
pipeline1/input2	0
pipeline1dup/input1	1
pipeline1dup/input2	0
`), 0600)
		c.Assert(err, check.IsNil)
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-chunked-hgvs-matrix=true",
			"-chi2-case-control-file=" + tmpdir + "/casecontrol.tsv",
			"-chi2-case-control-column=CC",
			"-chi2-p-value=0.5",
			"-min-coverage=0.75",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		annotations, err := ioutil.ReadFile(npydir + "/hgvs.chr2.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Check(string(annotations), check.Equals, `0,chr2:g.470_472del
1,chr2:g.471G>A
2,chr2:g.472G>A
`)
	}

	c.Log("=== slice-numpy + onehotChunked ===")
	{
		err = ioutil.WriteFile(tmpdir+"/casecontrol.tsv", []byte(`SampleID	CC
pipeline1/input1	1
pipeline1/input2	0
pipeline1dup/input1	1
pipeline1dup/input2	0
`), 0600)
		c.Assert(err, check.IsNil)
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-chunked-onehot=true",
			"-chi2-case-control-file=" + tmpdir + "/casecontrol.tsv",
			"-chi2-case-control-column=CC",
			"-chi2-p-value=0.5",
			"-min-coverage=0.75",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		f, err := os.Open(npydir + "/onehot.0002.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err := gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{4, 6})
		onehot, err := npy.GetInt8()
		if c.Check(err, check.IsNil) {
			for r := 0; r < npy.Shape[0]; r++ {
				c.Logf("%v", onehot[r*npy.Shape[1]:(r+1)*npy.Shape[1]])
			}
			c.Check(onehot, check.DeepEquals, []int8{
				0, 0, 0, 1, 0, 0, // input1
				0, 1, 0, 0, 0, 1, // input2
				0, 0, 0, 1, 0, 0, // dup/input1
				0, 1, 0, 0, 0, 1, // dup/input2
			})
		}
	}

	c.Log("=== slice-numpy + onehotSingle ===")
	{
		err = ioutil.WriteFile(tmpdir+"/casecontrol.tsv", []byte(`SampleID	CC
pipeline1/input1	1
pipeline1/input2	0
pipeline1dup/input1	1
pipeline1dup/input2	0
`), 0600)
		c.Assert(err, check.IsNil)
		npydir := c.MkDir()
		exited := (&sliceNumpy{}).RunCommand("slice-numpy", []string{
			"-local=true",
			"-single-onehot=true",
			"-chi2-case-control-file=" + tmpdir + "/casecontrol.tsv",
			"-chi2-case-control-column=CC",
			"-chi2-p-value=0.5",
			"-min-coverage=0.75",
			"-input-dir=" + slicedir,
			"-output-dir=" + npydir,
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", npydir, "-ls").CombinedOutput()
		c.Logf("%s", out)

		f, err := os.Open(npydir + "/onehot.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err := gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{2, 16})
		onehot, err := npy.GetUint32()
		if c.Check(err, check.IsNil) {
			for r := 0; r < npy.Shape[0]; r++ {
				c.Logf("%v", onehot[r*npy.Shape[1]:(r+1)*npy.Shape[1]])
			}
			c.Check(onehot, check.DeepEquals, []uint32{
				0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 0, 2,
				1, 1, 2, 2, 5, 5, 7, 7, 9, 9, 11, 11, 13, 13, 15, 15,
			})
		}
	}
}
