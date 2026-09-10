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
	"encoding/json"
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	Name = "CCXAlign"

	driverName        = "dra.cpu"
	capacityName      = "dra.cpu/cpu"
	numaAttribute     = "dra.cpu/numaNodeID"
	standardNUMAAttr  = "resource.kubernetes.io/numaNode"
	cacheL3IDAttr     = "dra.cpu/cacheL3ID"
	partitionAttr     = "dra.cpu/partition"
	repairRoundsAttr  = "dra.cpu/repairRounds"
	frontierInputAttr = "dra.cpu/frontierInput"

	defaultPartition = "default"
	defaultCacheSize = 16

	TierWarmAligned = 80
	TierColdAligned = 40
	TierSplit       = 0

	MaxBonus = 8

	TierLabelWarmAligned = "warm-aligned"
	TierLabelRepairable  = "repairable"
	TierLabelColdAligned = "cold-aligned"
	TierLabelSplit       = "split"

	stateKey fwk.StateKey = Name
)

type ScoringStrategyType string

const (
	MostAllocated  ScoringStrategyType = "MostAllocated"
	LeastAllocated ScoringStrategyType = "LeastAllocated"
	Packing        ScoringStrategyType = "Packing"
	Spreading      ScoringStrategyType = "Spreading"
)

type ScoringStrategy struct {
	Type ScoringStrategyType `json:"type,omitempty"`
}

type CCXAlignArgs struct {
	metav1.TypeMeta `json:",inline"`
	ScoringStrategy *ScoringStrategy    `json:"scoringStrategy,omitempty"`
	Type            ScoringStrategyType `json:"type,omitempty"`
}

func (a *CCXAlignArgs) DeepCopyObject() runtime.Object {
	if a == nil {
		return nil
	}
	out := new(CCXAlignArgs)
	*out = *a
	if a.ScoringStrategy != nil {
		out.ScoringStrategy = new(ScoringStrategy)
		*out.ScoringStrategy = *a.ScoringStrategy
	}
	return out
}

func (a *CCXAlignArgs) Mode() ScoringStrategyType {
	if a != nil {
		if a.ScoringStrategy != nil && a.ScoringStrategy.Type != "" {
			switch a.ScoringStrategy.Type {
			case LeastAllocated, Spreading:
				return Spreading
			default:
				return Packing
			}
		}
		switch a.Type {
		case LeastAllocated, Spreading:
			return Spreading
		default:
			return Packing
		}
	}
	return Packing
}

func parseArgs(obj runtime.Object) (*CCXAlignArgs, error) {
	args := &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	}
	if obj == nil {
		return args, nil
	}
	switch o := obj.(type) {
	case *CCXAlignArgs:
		return o, nil
	case *runtime.Unknown:
		if len(o.Raw) > 0 {
			if err := json.Unmarshal(o.Raw, args); err != nil {
				return nil, err
			}
		}
	default:
		data, err := json.Marshal(obj)
		if err == nil {
			_ = json.Unmarshal(data, args)
		}
	}
	return args, nil
}

type alignState struct {
	claims        []*resourceapi.ResourceClaim
	mu            sync.Mutex
	predictedNUMA map[string]int
}

func newAlignState(claims []*resourceapi.ResourceClaim) *alignState {
	return &alignState{
		claims:        claims,
		predictedNUMA: make(map[string]int),
	}
}

func (s *alignState) Clone() fwk.StateData {
	s.mu.Lock()
	defer s.mu.Unlock()
	clonedNUMA := make(map[string]int, len(s.predictedNUMA))
	for k, v := range s.predictedNUMA {
		clonedNUMA[k] = v
	}
	return &alignState{
		claims:        append([]*resourceapi.ResourceClaim(nil), s.claims...),
		predictedNUMA: clonedNUMA,
	}
}

func (s *alignState) setPredictedNUMA(nodeName string, numaID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.predictedNUMA[nodeName] = numaID
}

func (s *alignState) getPredictedNUMA(nodeName string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	numaID, ok := s.predictedNUMA[nodeName]
	return numaID, ok
}
