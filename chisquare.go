// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

var chisquared = distuv.ChiSquared{K: 1, Src: rand.NewSource(rand.Uint64())}

func pvalue(x, y []bool) float64 {
	var (
		obs, exp [2]float64
		sum      float64
		sz       = float64(len(y))
	)
	for i, yi := range y {
		if x[i] {
			if yi {
				obs[0]++
			} else {
				obs[1]++
			}
		}
		if yi {
			exp[0]++
		} else {
			exp[1]++
		}
	}
	if exp[0] == 0 || exp[1] == 0 || obs[0]+obs[1] == 0 {
		return 1
	}
	exp[0] = (obs[0] + obs[1]) * exp[0] / sz
	exp[1] = (obs[0] + obs[1]) * exp[1] / sz
	for i := range exp {
		d := obs[i] - exp[i]
		sum += d * d / exp[i]
	}
	return 1 - chisquared.CDF(sum)
}
