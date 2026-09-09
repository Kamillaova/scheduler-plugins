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
	resourceapi "k8s.io/api/resource/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
)

// nodePolicy is what a node's ResourceSlices say about how the driver
// allocates there: its request policy, and which placement policy the
// annotation may override.
type nodePolicy struct {
	minCPUs   int
	step      int
	placement string
}

// round is the demand the allocator will actually debit for a request: the
// device's request policy resolves rounding at scheduling time.
func (p nodePolicy) round(cpus int) int {
	if cpus < p.minCPUs {
		cpus = p.minCPUs
	}
	if p.step > 1 {
		cpus = (cpus + p.step - 1) / p.step * p.step
	}
	return cpus
}

// deviceOrder is the NUMA node ids in the order the driver published their
// devices, plus the node's request policy. The structured allocator's
// first-fit search walks devices in exactly this order -- the driver publishes
// one pool with one slice per node -- so this is what makes the plugin's NUMA
// prediction match the allocation that will actually happen.
func (p *CCXAlign) deviceOrder(nodeName string) ([]int, nodePolicy) {
	policy := nodePolicy{placement: policyPack}
	adopted := false
	var order []int
	for _, slice := range p.slices.forNode(nodeName) {
		if slice.Spec.Driver != driverName {
			continue
		}
		for _, device := range slice.Spec.Devices {
			attr, ok := device.Attributes[numaAttribute]
			if !ok || attr.IntValue == nil {
				continue
			}
			order = append(order, int(*attr.IntValue))
			capacity, ok := device.Capacity[capacityName]
			if !ok || capacity.RequestPolicy == nil || capacity.RequestPolicy.ValidRange == nil {
				continue
			}
			// Min stands on its own: ValidRange requires it while Step is
			// optional, so a driver may legally publish a minimum with no
			// step. The first device carrying a policy speaks for the node --
			// they cannot disagree under one driver, and letting whichever
			// the index returned last win would make rounding depend on
			// informer ordering.
			if adopted {
				continue
			}
			adopted = true
			valid := capacity.RequestPolicy.ValidRange
			if valid.Min != nil {
				policy.minCPUs = int(valid.Min.Value())
			}
			if valid.Step != nil {
				policy.step = int(valid.Step.Value())
			}
		}
	}
	return order, policy
}

// informerClaimLister adapts the shared claim lister.
type informerClaimLister struct {
	lister resourcelisters.ResourceClaimLister
}

func (l informerClaimLister) get(namespace, name string) (*resourceapi.ResourceClaim, error) {
	return l.lister.ResourceClaims(namespace).Get(name)
}

// informerSliceLister indexes ResourceSlices by node name, so a Score call
// does not list the world. The index is registered at construction, before
// the framework starts the informer; AddIndexers is rejected afterwards.
type informerSliceLister struct {
	informer cache.SharedIndexInformer
}

const sliceNodeIndex = "ccxalign-node"

func newSliceLister(informer cache.SharedIndexInformer) (*informerSliceLister, error) {
	err := informer.AddIndexers(cache.Indexers{sliceNodeIndex: func(obj interface{}) ([]string, error) {
		slice, ok := obj.(*resourceapi.ResourceSlice)
		if !ok || slice.Spec.NodeName == nil {
			return nil, nil
		}
		return []string{*slice.Spec.NodeName}, nil
	}})
	if err != nil {
		return nil, err
	}
	return &informerSliceLister{informer: informer}, nil
}

func (l *informerSliceLister) forNode(nodeName string) []*resourceapi.ResourceSlice {
	objs, err := l.informer.GetIndexer().ByIndex(sliceNodeIndex, nodeName)
	if err != nil {
		return nil
	}
	slices := make([]*resourceapi.ResourceSlice, 0, len(objs))
	for _, obj := range objs {
		if slice, ok := obj.(*resourceapi.ResourceSlice); ok {
			slices = append(slices, slice)
		}
	}
	return slices
}
