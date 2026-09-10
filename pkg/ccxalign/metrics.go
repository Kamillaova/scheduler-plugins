/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccxalign

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	chosenTierTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "ccxalign",
			Name:           "chosen_tier_total",
			Help:           "Number of times each alignment tier was chosen for a scored node.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"tier"},
	)

	predictedVsActualMismatchesTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "ccxalign",
			Name:           "predicted_vs_actual_mismatches_total",
			Help:           "Number of times the predicted landing NUMA node did not match the actual allocation.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)

	failClosedTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      "ccxalign",
			Name:           "fail_closed_total",
			Help:           "Number of fail-closed events when evaluating node alignment.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)

	registerMetricsOnce sync.Once
)

func initMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(
			chosenTierTotal,
			predictedVsActualMismatchesTotal,
			failClosedTotal,
		)
	})
}
