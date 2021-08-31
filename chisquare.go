// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

var chisquared = distuv.ChiSquared{K: 1, Src: rand.NewSource(rand.Uint64())}

func pvalue(a, b []bool) float64 {
	//     !b        b
	// !a  tab[0]    tab[1]
	// a   tab[2]    tab[3]
	tab := make([]int, 4)
	for ai, aval := range []bool{false, true} {
		for bi, bval := range []bool{false, true} {
			obs := 0
			for i := range a {
				if a[i] == aval && b[i] == bval {
					obs++
				}
			}
			tab[ai*2+bi] = obs
		}
	}
	var sum float64
	for ai := 0; ai < 2; ai++ {
		for bi := 0; bi < 2; bi++ {
			rowtotal := tab[ai*2] + tab[ai*2+1]
			coltotal := tab[bi] + tab[2+bi]
			exp := float64(rowtotal) * float64(coltotal) / float64(len(a))
			obs := tab[ai*2+bi]
			d := float64(obs) - exp
			sum += (d * d) / exp
		}
	}
	return 1 - chisquared.CDF(sum)
}
