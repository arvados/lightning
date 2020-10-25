package main

import (
	"bytes"
	"strings"

	"gopkg.in/check.v1"
)

type tilelibSuite struct {
	tag    []string
	taglib tagLibrary
}

var _ = check.Suite(&tilelibSuite{})

func (s *tilelibSuite) SetUpSuite(c *check.C) {
	fasta := `>0000.00
ggagaactgtgctccgccttcaga
acacatgctagcgcgtcggggtgg
gactctagcagagtggccagccac
cctcccgagccgagccacccgtca
gttattaataataacttatcatca
`
	err := s.taglib.Load(bytes.NewBufferString(fasta))
	c.Assert(err, check.IsNil)
	for _, seq := range strings.Split(fasta, "\n") {
		if len(seq) > 0 && seq[0] != '>' {
			s.tag = append(s.tag, seq+"\n")
		}
	}
}

func (s *tilelibSuite) TestSkipOOO(c *check.C) {
	// tags appear in seq: 4, 0, 2 (but skipOOO is false)
	tilelib := &tileLibrary{taglib: &s.taglib, skipOOO: false}
	tseq, _, err := tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[4]+
		"ggggggggggggggggggggggg\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[2]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{4, 1}, {0, 1}, {2, 1}}})

	// tags appear in seq: 0, 1, 2 -> don't skip
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[1]+
		"ggggggggggggggggggggggg\n"+
		s.tag[2]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {2, 1}}})

	// tags appear in seq: 2, 3, 4 -> don't skip
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[2]+
		"cccccccccccccccccccc\n"+
		s.tag[3]+
		"ggggggggggggggggggggggg\n"+
		s.tag[4]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{2, 1}, {3, 1}, {4, 1}}})

	// tags appear in seq: 4, 0, 2 -> skip 4
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[4]+
		"cccccccccccccccccccc\n"+
		s.tag[0]+
		"ggggggggggggggggggggggg\n"+
		s.tag[2]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {2, 1}}})

	// tags appear in seq: 0, 2, 1 -> skip 2
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[2]+
		"ggggggggggggggggggggggg\n"+
		s.tag[1]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}}})

	// tags appear in seq: 0, 1, 1, 2 -> skip second tag1
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[1]+
		"ggggggggggggggggggggggg\n"+
		s.tag[1]+
		"ggggggggggggggggggggggg\n"+
		s.tag[2]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {2, 1}}})

	// tags appear in seq: 0, 1, 3, 0, 4 -> skip second tag0
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[1]+
		"ggggggggggggggggggggggg\n"+
		s.tag[3]+
		"ggggggggggggggggggggggg\n"+
		s.tag[0]+
		"ggggggggggggggggggggggg\n"+
		s.tag[4]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {3, 1}, {4, 1}}})

	// tags appear in seq: 0, 1, 3 -> don't skip
	tilelib = &tileLibrary{taglib: &s.taglib, skipOOO: true}
	tseq, _, err = tilelib.TileFasta("test-label", bytes.NewBufferString(">test-seq\n"+
		s.tag[0]+
		"cccccccccccccccccccc\n"+
		s.tag[1]+
		"ggggggggggggggggggggggg\n"+
		s.tag[3]+
		"\n"))
	c.Assert(err, check.IsNil)
	c.Check(tseq, check.DeepEquals, tileSeq{"test-seq": []tileLibRef{{0, 1}, {1, 1}, {3, 1}}})
}
