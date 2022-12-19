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
func glmPvalueFunc(sampleInfo []sampleInfo, nPCA int) func(onehot []bool) float64 {
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

	outcome := make([]statmodel.Dtype, 0, len(sampleInfo))
	constants := make([]statmodel.Dtype, 0, len(sampleInfo))
	row := 0
	for _, si := range sampleInfo {
		if si.isTraining {
			if si.isCase {
				outcome = append(outcome, 1)
			} else {
				outcome = append(outcome, 0)
			}
			constants = append(constants, 1)
			row++
		}
	}
	data = append([][]statmodel.Dtype{outcome, constants}, data...)
	names := append([]string{"outcome", "constants"}, pcaNames...)
	dataset := statmodel.NewDataset(data, names)

	model, err := glm.NewGLM(dataset, "outcome", names[1:], glmConfig)
	if err != nil {
		log.Printf("%s", err)
		return func([]bool) float64 { return math.NaN() }
	}
	resultCov := model.Fit()
	logCov := resultCov.LogLike()

	return func(onehot []bool) (p float64) {
		defer func() {
			if recover() != nil {
				// typically "matrix singular or near-singular with condition number +Inf"
				p = math.NaN()
			}
		}()

		variant := make([]statmodel.Dtype, 0, len(sampleInfo))
		row := 0
		for _, si := range sampleInfo {
			if si.isTraining {
				if onehot[row] {
					variant = append(variant, 1)
				} else {
					variant = append(variant, 0)
				}
				row++
			}
		}

		data := append([][]statmodel.Dtype{data[0], variant}, data[1:]...)
		names := append([]string{"outcome", "variant"}, names[1:]...)
		dataset := statmodel.NewDataset(data, names)

		model, err := glm.NewGLM(dataset, "outcome", names[1:], glmConfig)
		if err != nil {
			return math.NaN()
		}
		resultComp := model.Fit()
		logComp := resultComp.LogLike()
		dist := distuv.ChiSquared{K: 1}
		return dist.Survival(-2 * (logCov - logComp))
	}
}
