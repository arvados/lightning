// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"bytes"
	"io/ioutil"
	"os"

	"gopkg.in/check.v1"
)

type diffSuite struct{}

var _ = check.Suite(&diffSuite{})

func (s *diffSuite) TestDiff(c *check.C) {
	tempdir, err := ioutil.TempDir("", "")
	c.Assert(err, check.IsNil)
	defer os.RemoveAll(tempdir)

	err = ioutil.WriteFile(tempdir+"/f1.fa", []byte(">f1\nactgactCacgtacgt\nactgactgacgAAcgt\n"), 0700)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(tempdir+"/f2.fa", []byte(">f2\nactgactGacgtacgt\nactgactgacgTTcgtA\n"), 0700)
	c.Assert(err, check.IsNil)

	var output bytes.Buffer
	exited := (&diffFasta{}).RunCommand("diff-fasta", []string{"-sequence", "chr2", "-offset", "1000", tempdir + "/f1.fa", tempdir + "/f2.fa"}, nil, &output, os.Stderr)
	c.Check(exited, check.Equals, 0)
	c.Check("\n"+output.String(), check.Equals, `
chr2:g.1008C>G	chr2	1008	C	G	false
chr2:g.1028_1029delinsTT	chr2	1028	AA	TT	false
chr2:g.1032_1033insA	chr2	1033		A	false
`)
}
