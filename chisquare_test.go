// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"fmt"

	"gopkg.in/check.v1"
)

type pvalueSuite struct{}

var _ = check.Suite(&pvalueSuite{})

func (s *pvalueSuite) TestPvalue(c *check.C) {
	a := make([]bool, 54)
	b := make([]bool, 54)
	for i := 0; i < 25; i++ {
		a[i] = true
		b[i] = true
	}
	for i := 25; i < 31; i++ {
		a[i] = true
	}
	for i := 31; i < 39; i++ {
		b[i] = true
	}
	c.Check(fmt.Sprintf("%.7f", pvalue(a, b)), check.Equals, "0.0006297")
	for i := range a {
		a[i] = !a[i]
	}
	c.Check(fmt.Sprintf("%.7f", pvalue(a, b)), check.Equals, "0.0006297")
}
