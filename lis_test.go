// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"gopkg.in/check.v1"
)

type lisSuite struct{}

var _ = check.Suite(&lisSuite{})

func (s *lisSuite) Test(c *check.C) {
	for _, trial := range []struct {
		in  []int
		out []int
	}{
		{},
		{
			in: []int{},
		},
		{
			in:  []int{0},
			out: []int{0},
		},
		{
			in:  []int{1, 2, 3, 4},
			out: []int{0, 1, 2, 3},
		},
		{
			in:  []int{1, 2, 2, 4},
			out: []int{0, 2, 3},
		},
		{
			in:  []int{4, 3, 2, 1},
			out: []int{3},
		},
		{
			in:  []int{1, 3, 2, 4},
			out: []int{0, 2, 3},
		},
		{
			in:  []int{1, 0, 0, 0, 4},
			out: []int{3, 4},
		},
		{
			in:  []int{0, 1, 2, 1, 4, 5},
			out: []int{0, 1, 2, 4, 5},
		},
	} {
		c.Logf("=== %v", trial)
		c.Check(longestIncreasingSubsequence(len(trial.in), func(i int) int { return trial.in[i] }), check.DeepEquals, trial.out)
	}
}
