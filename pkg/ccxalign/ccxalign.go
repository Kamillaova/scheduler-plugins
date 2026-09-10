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
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
)

type CCXAlign struct {
	handle  fwk.Handle
	args    *CCXAlignArgs
	claims  claimTracker
	classes classLister
	slices  sliceLister
}

type claimTracker interface {
	Get(namespace, name string) (*resourceapi.ResourceClaim, error)
	GatherAllocatedState() (*structured.AllocatedState, error)
	List() ([]*resourceapi.ResourceClaim, error)
}

type classLister interface {
	Get(name string) (*resourceapi.DeviceClass, error)
}

type sliceLister interface {
	forNode(nodeName string) []*resourceapi.ResourceSlice
}

type draManagerClaimTracker struct {
	tracker fwk.ResourceClaimTracker
}

func (t draManagerClaimTracker) Get(namespace, name string) (*resourceapi.ResourceClaim, error) {
	return t.tracker.Get(namespace, name)
}

func (t draManagerClaimTracker) GatherAllocatedState() (*structured.AllocatedState, error) {
	return t.tracker.GatherAllocatedState()
}

func (t draManagerClaimTracker) List() ([]*resourceapi.ResourceClaim, error) {
	return t.tracker.List()
}

type draManagerClassLister struct {
	lister fwk.DeviceClassLister
}

func (l draManagerClassLister) Get(name string) (*resourceapi.DeviceClass, error) {
	return l.lister.Get(name)
}

var (
	_ fwk.PreFilterPlugin = &CCXAlign{}
	_ fwk.PreScorePlugin  = &CCXAlign{}
	_ fwk.ScorePlugin     = &CCXAlign{}
)

func New(_ context.Context, configuration runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	initMetrics()

	args, err := parseArgs(configuration)
	if err != nil {
		return nil, fmt.Errorf("parsing CCXAlign configuration: %w", err)
	}

	factory := handle.SharedInformerFactory()
	slices, err := newSliceLister(factory.Resource().V1().ResourceSlices().Informer())
	if err != nil {
		return nil, fmt.Errorf("indexing ResourceSlices by node: %w", err)
	}

	plugin := &CCXAlign{
		handle: handle,
		args:   args,
		slices: slices,
	}

	if mgr := handle.SharedDRAManager(); mgr != nil {
		plugin.claims = draManagerClaimTracker{tracker: mgr.ResourceClaims()}
		plugin.classes = draManagerClassLister{lister: mgr.DeviceClasses()}
	}

	return plugin, nil
}

func (p *CCXAlign) Name() string { return Name }

func (p *CCXAlign) PreFilter(_ context.Context, state fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	claims := p.podClaims(pod)
	if len(claims) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	state.Write(stateKey, newAlignState(claims))
	return nil, nil
}

func (p *CCXAlign) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

func (p *CCXAlign) podClaims(pod *v1.Pod) []*resourceapi.ResourceClaim {
	if p.claims == nil {
		return nil
	}
	var claims []*resourceapi.ResourceClaim
	for i := range pod.Spec.ResourceClaims {
		claimName, _, err := resourceclaim.Name(pod, &pod.Spec.ResourceClaims[i])
		if err != nil || claimName == nil {
			continue
		}
		claim, err := p.claims.Get(pod.Namespace, *claimName)
		if err != nil || claim == nil {
			continue
		}
		if p.isDriverClaim(claim) {
			claims = append(claims, claim)
		}
	}
	return claims
}

func (p *CCXAlign) isDriverClaim(claim *resourceapi.ResourceClaim) bool {
	if claim.Status.Allocation != nil {
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver == driverName {
				return true
			}
		}
	}
	for _, req := range claim.Spec.Devices.Requests {
		if req.Exactly != nil {
			if p.isDriverClass(req.Exactly.DeviceClassName) {
				return true
			}
		}
		for _, sub := range req.FirstAvailable {
			if p.isDriverClass(sub.DeviceClassName) {
				return true
			}
		}
	}
	return false
}

func (p *CCXAlign) isDriverClass(className string) bool {
	if p.classes == nil {
		return className == driverName
	}
	class, err := p.classes.Get(className)
	if err != nil || class == nil {
		return false
	}
	for _, config := range class.Spec.Config {
		if config.Opaque != nil && config.Opaque.Driver == driverName {
			return true
		}
	}
	for _, sel := range class.Spec.Selectors {
		if strings.Contains(sel.CEL.Expression, driverName) {
			return true
		}
	}
	return class.Name == driverName
}

func (p *CCXAlign) PreScore(_ context.Context, state fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) *fwk.Status {
	if _, err := state.Read(stateKey); err != nil {
		return fwk.NewStatus(fwk.Skip)
	}
	return nil
}

func (p *CCXAlign) Score(_ context.Context, state fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	data, err := state.Read(stateKey)
	if err != nil {
		return TierSplit, nil
	}
	alignData := data.(*alignState)

	node := nodeInfo.Node()
	if node == nil {
		return TierSplit, nil
	}

	rawDevices, policy := p.nodeDevices(node.Name)
	if len(rawDevices) == 0 {
		return TierSplit, nil
	}

	var allocatedState *structured.AllocatedState
	if p.claims != nil {
		allocatedState, _ = p.claims.GatherAllocatedState()
	}

	mode := Packing
	if p.args != nil {
		mode = p.args.Mode()
	}

	score, tierLabel, predictedLanding := scorePodOnNode(alignData.claims, rawDevices, allocatedState, policy, mode)
	if predictedLanding != -1 {
		alignData.setPredictedNUMA(node.Name, predictedLanding)
	}

	chosenTierTotal.WithLabelValues(tierLabel).Inc()
	return score, nil
}

func (p *CCXAlign) ScoreExtensions() fwk.ScoreExtensions { return nil }
