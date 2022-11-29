// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"fmt"
	"math/rand"

	"gopkg.in/check.v1"
)

type glmSuite struct{}

var _ = check.Suite(&glmSuite{})

func (s *glmSuite) TestPvalue(c *check.C) {
	c.Check(pvalueGLM([]sampleInfo{
		{id: "sample1", isCase: false, isTraining: true, pcaComponents: []float64{-4, 1.2, -3}},
		{id: "sample2", isCase: false, isTraining: true, pcaComponents: []float64{7, -1.2, 2}},
		{id: "sample3", isCase: true, isTraining: true, pcaComponents: []float64{7, -1.2, 2}},
		{id: "sample4", isCase: true, isTraining: true, pcaComponents: []float64{-4, 1.1, -2}},
	}, [][]bool{
		{false, false, true, true},
		{false, false, true, true},
	}), check.Equals, 0.09589096738494937)

	c.Check(pvalueGLM([]sampleInfo{
		{id: "sample1", isCase: false, isTraining: true, pcaComponents: []float64{1, 1.21, 2.37}},
		{id: "sample2", isCase: false, isTraining: true, pcaComponents: []float64{2, 1.22, 2.38}},
		{id: "sample3", isCase: false, isTraining: true, pcaComponents: []float64{3, 1.23, 2.39}},
		{id: "sample4", isCase: false, isTraining: true, pcaComponents: []float64{1, 1.24, 2.33}},
		{id: "sample5", isCase: false, isTraining: true, pcaComponents: []float64{2, 1.25, 2.34}},
		{id: "sample6", isCase: true, isTraining: true, pcaComponents: []float64{3, 1.26, 2.35}},
		{id: "sample7", isCase: true, isTraining: true, pcaComponents: []float64{1, 1.23, 2.36}},
		{id: "sample8", isCase: true, isTraining: true, pcaComponents: []float64{2, 1.22, 2.32}},
		{id: "sample9", isCase: true, isTraining: true, pcaComponents: []float64{3, 1.21, 2.31}},
	}, [][]bool{
		{false, false, false, false, false, true, true, true, true},
		{false, false, false, false, false, true, true, true, true},
	}), check.Equals, 0.001028375654911555)

	c.Check(pvalueGLM([]sampleInfo{
		{id: "sample1", isCase: false, isTraining: true, pcaComponents: []float64{1.001, -1.01, 2.39}},
		{id: "sample2", isCase: false, isTraining: true, pcaComponents: []float64{1.002, -1.02, 2.38}},
		{id: "sample3", isCase: false, isTraining: true, pcaComponents: []float64{1.003, -1.03, 2.37}},
		{id: "sample4", isCase: false, isTraining: true, pcaComponents: []float64{1.004, -1.04, 2.36}},
		{id: "sample5", isCase: false, isTraining: true, pcaComponents: []float64{1.005, -1.05, 2.35}},
		{id: "sample6", isCase: false, isTraining: true, pcaComponents: []float64{1.006, -1.06, 2.34}},
		{id: "sample7", isCase: false, isTraining: true, pcaComponents: []float64{1.007, -1.07, 2.33}},
		{id: "sample8", isCase: false, isTraining: true, pcaComponents: []float64{1.008, -1.08, 2.32}},
		{id: "sample9", isCase: false, isTraining: false, pcaComponents: []float64{2.000, 8.01, -2.01}},
		{id: "sample10", isCase: true, isTraining: true, pcaComponents: []float64{2.001, 8.02, -2.02}},
		{id: "sample11", isCase: true, isTraining: true, pcaComponents: []float64{2.002, 8.03, -2.03}},
		{id: "sample12", isCase: true, isTraining: true, pcaComponents: []float64{2.003, 8.04, -2.04}},
		{id: "sample13", isCase: true, isTraining: true, pcaComponents: []float64{2.004, 8.05, -2.05}},
		{id: "sample14", isCase: true, isTraining: true, pcaComponents: []float64{2.005, 8.06, -2.06}},
		{id: "sample15", isCase: true, isTraining: true, pcaComponents: []float64{2.006, 8.07, -2.07}},
		{id: "sample16", isCase: true, isTraining: true, pcaComponents: []float64{2.007, 8.08, -2.08}},
		{id: "sample17", isCase: true, isTraining: true, pcaComponents: []float64{2.008, 8.09, -2.09}},
		{id: "sample18", isCase: true, isTraining: true, pcaComponents: []float64{2.009, 8.10, -2.10}},
		{id: "sample19", isCase: true, isTraining: true, pcaComponents: []float64{2.010, 8.11, -2.11}},
	}, [][]bool{
		{false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
		{false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true, true, true},
	}), check.Equals, 0.9999944849940106)
}

var benchSamples, benchOnehot = func() ([]sampleInfo, [][]bool) {
	pcaComponents := 10
	samples := []sampleInfo{}
	onehot := make([][]bool, 2)
	r := make([]float64, pcaComponents)
	for j := 0; j < 10000; j++ {
		for i := 0; i < len(r); i++ {
			r[i] = rand.Float64()
		}
		samples = append(samples, sampleInfo{
			id:            fmt.Sprintf("sample%d", j),
			isCase:        j%2 == 0 && j > 200,
			isControl:     j%2 == 1 || j <= 200,
			isTraining:    true,
			pcaComponents: append([]float64(nil), r...),
		})
		onehot[0] = append(onehot[0], j%2 == 0)
		onehot[1] = append(onehot[1], j%2 == 0)
	}
	return samples, onehot
}()

func (s *glmSuite) BenchmarkPvalue(c *check.C) {
	for i := 0; i < c.N; i++ {
		p := pvalueGLM(benchSamples, benchOnehot)
		c.Check(p, check.Equals, 0.0)
	}
}
