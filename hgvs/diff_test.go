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
			expect: []string{"3G>C", "4G>C", "7_8del", "12_13insC"},
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
			expect: []string{"3G>A", "4G>A"},
		},
		{
			// without cleanup, diffmatchpatch solves this as {"3_4del", "=A", "5_6insCA"}
			a:      "agggaggggg",
			b:      "agACaggggg",
			expect: []string{"3G>A", "4G>C"},
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
			expect: []string{"3G>C", "4G>A"},
		},
		{
			a:      "agggg",
			b:      "agAAg",
			expect: []string{"3G>A", "4G>A"},
		},
		{
			a:      "aggggg",
			b:      "agAAAg",
			expect: []string{"3_5delinsAAA"},
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
		{
			a:      "tcagatggac",
			b:      "tcaAaCggac",
			expect: []string{"4G>A", "6T>C"},
		},
		{
			a:      "tcagatggac",
			b:      "tcaAaCggTc",
			expect: []string{"4G>A", "6T>C", "9A>T"},
		},
		{
			a:      "tcagatggac",
			b:      "tcaAaCCggTc",
			expect: []string{"4G>A", "6delinsCC", "9A>T"},
		},
		{
			a:      "tcatagagac",
			b:      "tcacaagac",
			expect: []string{"4T>C", "6del"},
		},
		{
			a:      "tcatcgagac",
			b:      "tcGCcgagac",
			expect: []string{"3A>G", "4T>C"},
		},
		{
			a:      "tcatcgagac",
			b:      "tcGCcggac",
			expect: []string{"3A>G", "4T>C", "7del"},
		},
		{
			// should delete leftmost
			a:      "acgacaTTtttacac",
			b:      "acgacatttacac",
			expect: []string{"7_8del"},
		},
		{
			// should delete leftmost
			a:      "acgacATatatacac",
			b:      "acgacatatacac",
			expect: []string{"6_7del"},
		},
		{
			// should insert leftmost
			a:      "acgacatttacac",
			b:      "acgacaTTtttacac",
			expect: []string{"6_7insTT"},
		},
		{
			// should insert leftmost
			a:      "acgacatatacac",
			b:      "acgacATatatacac",
			expect: []string{"5_6insAT"},
		},
		{
			a:      "cccacGATAtatcc",
			b:      "cccactatcc",
			expect: []string{"6_9del"},
		},
		{
			a:      "acGATAtatcc",
			b:      "actatcc",
			expect: []string{"3_6del"},
		},
		{
			a:      "acTTTTTatcc",
			b:      "acGTTTatcc",
			expect: []string{"3_4delinsG"},
		},
		{
			a:      "acTTTTatcc",
			b:      "acGTTTTTatcc",
			expect: []string{"2_3insGT"},
		},
		{
			a:      "aGACGGACAGGGCCCgg",
			b:      "agg",
			expect: []string{"3_15del"},
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
