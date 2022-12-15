// Copyright (C) The Lightning Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

package lightning

import (
	"fmt"
	"io"
	"log"
	"math"

	"github.com/kshedden/statmodel/glm"
	"github.com/kshedden/statmodel/statmodel"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
)

var glmConfig = &glm.Config{
	Family:         glm.NewFamily(glm.BinomialFamily),
	FitMethod:      "IRLS",
	ConcurrentIRLS: 1000,
	Log:            log.New(io.Discard, "", 0),
}

func normalize(a []float64) {
	mean, std := stat.MeanStdDev(a, nil)
	for i, x := range a {
		a[i] = (x - mean) / std
	}
}

// Logistic regression.
//
// onehot is the observed outcome, in same order as sampleInfo, but
// shorter because it only has entries for samples with
// isTraining==true.
func pvalueGLM(sampleInfo []sampleInfo, onehot []bool, nPCA int) (p float64) {
	pcaNames := make([]string, 0, nPCA)
	data := make([][]statmodel.Dtype, 0, nPCA)
	for pca := 0; pca < nPCA; pca++ {
		series := make([]statmodel.Dtype, 0, len(sampleInfo))
		for _, si := range sampleInfo {
			if si.isTraining {
				series = append(series, si.pcaComponents[pca])
			}
		}
		normalize(series)
		data = append(data, series)
		pcaNames = append(pcaNames, fmt.Sprintf("pca%d", pca))
	}

	variant := make([]statmodel.Dtype, 0, len(sampleInfo))
	outcome := make([]statmodel.Dtype, 0, len(sampleInfo))
	constants := make([]statmodel.Dtype, 0, len(sampleInfo))
	row := 0
	for _, si := range sampleInfo {
		if si.isTraining {
			if onehot[row] {
				variant = append(variant, 1)
			} else {
				variant = append(variant, 0)
			}
			if si.isCase {
				outcome = append(outcome, 1)
			} else {
				outcome = append(outcome, 0)
			}
			constants = append(constants, 1)
			row++
		}
	}
	data = append(data, variant, outcome, constants)
	dataset := statmodel.NewDataset(data, append(pcaNames, "variant", "outcome", "constants"))

	defer func() {
		if recover() != nil {
			// typically "matrix singular or near-singular with condition number +Inf"
			p = math.NaN()
		}
	}()
	model, err := glm.NewGLM(dataset, "outcome", append([]string{"constants"}, pcaNames...), glmConfig)
	if err != nil {
		return math.NaN()
	}
	resultCov := model.Fit()
	logCov := resultCov.LogLike()
	model, err = glm.NewGLM(dataset, "outcome", append([]string{"constants", "variant"}, pcaNames...), glmConfig)
	if err != nil {
		return math.NaN()
	}
	resultComp := model.Fit()
	logComp := resultComp.LogLike()
	dist := distuv.ChiSquared{K: 1}
	return dist.Survival(-2 * (logCov - logComp))
}
