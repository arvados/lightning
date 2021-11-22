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

		annotations, err = ioutil.ReadFile(npydir + "/matrix.0002.annotations.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", annotations)
		c.Check(string(annotations), check.Equals, "")
	}
}
