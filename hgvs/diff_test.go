// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package hgvs

import (
	"strings"
	"testing"

	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type diffSuite struct{}

var _ = check.Suite(&diffSuite{})

func (s *diffSuite) TestDiff(c *check.C) {
	for _, trial := range []struct {
		a      string
		b      string
		expect []string
	}{
		{
			a:      "aaaaaaaaaa",
			b:      "aaaaCaaaaa",
			expect: []string{"5A>C"},
		},
		{
			a:      "aaaacGcaaa",
			b:      "aaaaccaaa",
			expect: []string{"6del"},
		},
		{
			a:      "aaaacGGcaaa",
			b:      "aaaaccaaa",
			expect: []string{"6_7del"},
		},
		{
			a:      "aaaac",
			b:      "aaaa",
			expect: []string{"5del"},
		},
		{
			a:      "aaaa",
			b:      "aaCaa",
			expect: []string{"2_3insC"},
		},
		{
			a:      "aaGGGtt",
			b:      "aaCCCtt",
			expect: []string{"3_5delinsCCC"},
		},
		{
			a:      "aa",
			b:      "aaCCC",
			expect: []string{"2_3insCCC"},
		},
		{
			a:      "aaGGttAAtttt",
			b:      "aaCCttttttC",
			expect: []string{"3_4delinsCC", "7_8del", "12_13insC"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3del", "=A", "4_5insA"}
			a:      "aggaggggg",
			b:      "agAaggggg",
			expect: []string{"3G>A"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3_4del", "=A", "5_6insAA"}
			a:      "agggaggggg",
			b:      "agAAaggggg",
			expect: []string{"3_4delinsAA"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3_4del", "=A", "5_6insCA"}
			a:      "agggaggggg",
			b:      "agACaggggg",
			expect: []string{"3_4delinsAC"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3_7del", "=A", "8_9insAAACA"}
			a:      "aggggggaggggg",
			b:      "agAAAACaggggg",
			expect: []string{"3_7delinsAAAAC"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3_7del", "=AAAA", "11_12insCAAAA"}
			a:      "aggggggaaaaggggg",
			b:      "agAAAACaaaaggggg",
			expect: []string{"3_7delinsAAAAC"},
		},
		{
			a:      "agggaggggg",
			b:      "agCAaggggg",
			expect: []string{"3_4delinsCA"},
		},
		{
			a:      "agggg",
			b:      "agAAg",
			expect: []string{"3_4delinsAA"},
		},
		{
			a:      "acgtgaa",
			b:      "acTtgaa",
			expect: []string{"3G>T"},
		},
		{
			a:      "tcagaagac",
			b:      "tcaAaagac",
			expect: []string{"4G>A"},
		},
	} {
		c.Log(trial)
		var vars []string
		diffs, _ := Diff(strings.ToUpper(trial.a), strings.ToUpper(trial.b), 0)
		for _, v := range diffs {
			vars = append(vars, v.String())
		}
		c.Check(vars, check.DeepEquals, trial.expect)
	}
}
