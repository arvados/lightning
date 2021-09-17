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

	err := ioutil.WriteFile(tmpdir+"/chr1-12-100.bed", []byte("chr1\t12\t100\ttest.1\n"), 0644)
	c.Check(err, check.IsNil)

	c.Log("=== import testdata/ref ===")
	exited := (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-save-incomplete-tiles",
		"-o", tmpdir + "/library1.gob",
		"testdata/ref.fasta",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	c.Log("=== import testdata/pipeline1 ===")
	exited = (&importer{}).RunCommand("import", []string{
		"-local=true",
		"-tag-library", "testdata/tags",
		"-output-tiles",
		"-o", tmpdir + "/library2.gob",
		"testdata/pipeline1",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	c.Log("=== merge ===")
	exited = (&merger{}).RunCommand("merge", []string{
		"-local=true",
		"-o", tmpdir + "/library.gob",
		tmpdir + "/library1.gob",
		tmpdir + "/library2.gob",
	}, nil, os.Stderr, os.Stderr)
	c.Assert(exited, check.Equals, 0)

	input := tmpdir + "/library.gob"
	slicedir := c.MkDir()

	c.Log("=== slice ===")
	exited = (&slicecmd{}).RunCommand("slice", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + slicedir,
		"-tags-per-file=2",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	out, _ := exec.Command("find", slicedir, "-ls").CombinedOutput()
	c.Logf("%s", out)

	c.Log("=== slice-numpy ===")
	npydir := c.MkDir()
	exited = (&sliceNumpy{}).RunCommand("slice-numpy", []string{
		"-local=true",
		"-input-dir=" + slicedir,
		"-output-dir=" + npydir,
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	out, _ = exec.Command("find", npydir, "-ls").CombinedOutput()
	c.Logf("%s", out)

	f, err := os.Open(npydir + "/matrix.0000.npy")
	c.Assert(err, check.IsNil)
	defer f.Close()
	npy, err := gonpy.NewReader(f)
	c.Assert(err, check.IsNil)
	c.Check(npy.Shape, check.DeepEquals, []int{2, 4})
	variants, err := npy.GetInt16()
	c.Check(variants, check.DeepEquals, []int16{3, 2, 1, 2, -1, -1, 1, 1})

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
}
