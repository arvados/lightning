// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"fmt"
	"math"

	"github.com/kshedden/statmodel/glm"
	"github.com/kshedden/statmodel/statmodel"
)

var glmConfig = &glm.Config{
	Family:         glm.NewFamily(glm.BinomialFamily),
	FitMethod:      "IRLS",
	ConcurrentIRLS: 1000,
}

func pvalueGLM(sampleInfo []sampleInfo, onehotPair [][]bool) float64 {
	nPCA := len(sampleInfo[0].pcaComponents)
	pcaNames := make([]string, 0, nPCA)
	data := make([][]statmodel.Dtype, 0, nPCA)
	for pca := 0; pca < nPCA; pca++ {
		series := make([]statmodel.Dtype, 0, len(sampleInfo))
		for _, si := range sampleInfo {
			if si.isTraining {
				series = append(series, si.pcaComponents[pca])
			}
		}
		data = append(data, series)
		pcaNames = append(pcaNames, fmt.Sprintf("pca%d", pca))
	}

	variant := make([]statmodel.Dtype, 0, len(sampleInfo))
	outcome := make([]statmodel.Dtype, 0, len(sampleInfo))
	for row, si := range sampleInfo {
		if si.isTraining {
			if onehotPair[0][row] {
				variant = append(variant, 1)
			} else {
				variant = append(variant, 0)
			}
			if si.isCase {
				outcome = append(outcome, 1)
			} else {
				outcome = append(outcome, 0)
			}
		}
	}
	data = append(data, variant, outcome)

	dataset := statmodel.NewDataset(data, append(pcaNames, "variant", "outcome"))
	model, err := glm.NewGLM(dataset, "outcome", pcaNames, glmConfig)
	if err != nil {
		return math.NaN()
	}
	resultCov := model.Fit()
	logCov := resultCov.LogLike()
	model, err = glm.NewGLM(dataset, "outcome", append([]string{"variant"}, pcaNames...), glmConfig)
	if err != nil {
		return math.NaN()
	}
	resultComp := model.Fit()
	logComp := resultComp.LogLike()
	return chisquared.Survival(-2 * (logCov - logComp))
}
