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

	c.Log("=== dump ===")
	{
		dumpdir := c.MkDir()
		exited = (&dump{}).RunCommand("dump", []string{
			"-local=true",
			"-tags=4,6,7",
			"-input-dir=" + slicedir,
			"-output-dir=" + dumpdir,
		}, nil, os.Stderr, os.Stderr)
		c.Check(exited, check.Equals, 0)
		out, _ := exec.Command("find", dumpdir, "-ls").CombinedOutput()
		c.Logf("%s", out)
		dumped, err := ioutil.ReadFile(dumpdir + "/variants.csv")
		c.Assert(err, check.IsNil)
		c.Logf("%s", dumped)
		c.Check("\n"+string(dumped), check.Matches, `(?ms).*\n6,1,1,chr2,349,AAAACTG.*`)
	}

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
			c.Check(variants, check.DeepEquals, []int16{
				2, 1, 3, 1,
				-1, -1, 4, 2,
				2, 1, 3, 1,
				-1, -1, 4, 2,
			})
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
		c.Check(npy.Shape, check.DeepEquals, []int{4, 3})
		onehot, err := npy.GetInt8()
		if c.Check(err, check.IsNil) {
			for r := 0; r < npy.Shape[0]; r++ {
				c.Logf("%v", onehot[r*npy.Shape[1]:(r+1)*npy.Shape[1]])
			}
			c.Check(onehot, check.DeepEquals, []int8{
				0, 1, 0, // input1
				1, 0, 1, // input2
				0, 1, 0, // dup/input1
				1, 0, 1, // dup/input2
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
			"-debug-tag=1",
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
				0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7,
			})
		}

		f, err = os.Open(npydir + "/onehot-columns.npy")
		c.Assert(err, check.IsNil)
		defer f.Close()
		npy, err = gonpy.NewReader(f)
		c.Assert(err, check.IsNil)
		c.Check(npy.Shape, check.DeepEquals, []int{8, 5})
		onehotcols, err := npy.GetInt32()
		if c.Check(err, check.IsNil) {
			for r := 0; r < npy.Shape[0]; r++ {
				c.Logf("%v", onehotcols[r*npy.Shape[1]:(r+1)*npy.Shape[1]])
			}
			c.Check(onehotcols, check.DeepEquals, []int32{
				0, 0, 1, 4, 4, 4, 6, 6,
				2, 3, 2, 2, 3, 4, 2, 3,
				0, 1, 0, 0, 0, 0, 0, 0,
				157299, 157299, 157299, 157299, 157299, 157299, 157299, 157299,
				803273, 803273, 803273, 803273, 803273, 803273, 803273, 803273,
			})
		}
	}
}

func (s *sliceSuite) Test_tv2homhet(c *check.C) {
	cmd := &sliceNumpy{
		cgnames:         []string{"sample1", "sample2", "sample3", "sample4"},
		chi2Cases:       []bool{false, true, true, false},
		chi2PValue:      .5,
		includeVariant1: true,
	}
	cgs := map[string]CompactGenome{
		"sample1": CompactGenome{
			Variants: []tileVariantID{0, 0, 1, 1}, // hom tv=1
		},
		"sample2": CompactGenome{
			Variants: []tileVariantID{0, 0, 5, 5}, // hom tv=2
		},
		"sample3": CompactGenome{
			Variants: []tileVariantID{0, 0, 5, 1}, // het tv=1, tv=2
		},
		"sample4": CompactGenome{
			Variants: []tileVariantID{0, 0, 9, 9}, // hom tv=3
		},
	}
	maxv := tileVariantID(3)
	remap := []tileVariantID{0, 1, 0, 0, 0, 2, 0, 0, 0, 3}
	chunkstarttag := tagID(10)
	for tag := tagID(10); tag < 12; tag++ {
		c.Logf("=== tag %d", tag)
		chunk, xref := cmd.tv2homhet(cgs, maxv, remap, tag, chunkstarttag)
		c.Logf("chunk len=%d", len(chunk))
		for _, x := range chunk {
			c.Logf("%+v", x)
		}
		c.Logf("xref len=%d", len(xref))
		for _, x := range xref {
			c.Logf("%+v", x)
		}
		out := onehotcols2int8(chunk)
		c.Logf("onehotcols2int8(chunk) len=%d", len(out))
		for i := 0; i < len(out); i += len(chunk) {
			c.Logf("%+v", out[i:i+len(chunk)])
		}
		coords := onehotChunk2Indirect(chunk)
		c.Logf("onehotChunk2Indirect(chunk) len=%d", len(coords))
		for _, x := range coords {
			c.Logf("%+v", x)
		}
	}
}
