// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"io/ioutil"
	"os"
	"os/exec"

	"gopkg.in/check.v1"
)

type sliceSuite struct{}

var _ = check.Suite(&sliceSuite{})

func (s *sliceSuite) TestImportAndSlice(c *check.C) {
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

	exited = (&slicecmd{}).RunCommand("slice", []string{
		"-local=true",
		"-input-dir=" + input,
		"-output-dir=" + tmpdir,
		"-tags-per-file=2",
	}, nil, os.Stderr, os.Stderr)
	c.Check(exited, check.Equals, 0)
	out, _ := exec.Command("find", tmpdir, "-ls").CombinedOutput()
	c.Logf("%s", out)
}
