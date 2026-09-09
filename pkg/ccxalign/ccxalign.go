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

// Package ccxalign scores nodes by whether a DRA CPU claim can land aligned to
// uncore caches (AMD CCX / L3 domains) there. The scheduler's own view of a
// dra.cpu device is a free-capacity scalar that says nothing about the shape of
// the free CPUs, so the driver publishes the shape on a node annotation and
// this plugin prefers nodes where the claim fits on the fewest caches its size
// allows -- now, or after the node's defragmenter has repacked what it is
// actually willing to move.
//
// Score-only by design: fragmentation is mutable state, and filtering on it
// flaps pods Unschedulable. A cluster with no aligned home left still schedules
// the claim somewhere, and node-local defragmentation repairs what it can --
// exactly the documented fallback.
package ccxalign

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	// Name is the plugin name used in the scheduler configuration.
	Name = "CCXAlign"
	// deviceClassName is the DeviceClass whose claims this plugin understands.
	// It is a class name, not a driver name; they merely coincide for dra.cpu.
	deviceClassName = "dra.cpu"
	// driverName is the DRA driver whose ResourceSlices carry the device order
	// and request policy the scoring follows.
	driverName = "dra.cpu"
	// capacityName is the consumable capacity a claim requests.
	capacityName = "dra.cpu/cpu"
	// numaAttribute is the device attribute naming the NUMA node a device
	// covers, published by the driver on every grouped device.
	numaAttribute = "dra.cpu/numaNodeID"

	// reservationTTL bounds how long a reservation outlives its Reserve.
	// Nothing clears one on the happy path -- a bound claim stays allocated,
	// so neither informer path fires -- and the annotation, republished within
	// seconds of the driver preparing the claim, already counts it. Until the
	// TTL runs out the claim is therefore debited twice, which errs toward a
	// second-best node and never an infeasible one. Clearing earlier would
	// take knowing the annotation in hand was published after the reservation
	// was made, and the payload carries nothing to decide that. The claim
	// informer does clear a reservation whose claim dies instead of binding.
	reservationTTL = 60 * time.Second

	stateKey fwk.StateKey = Name
)

// CCXAlign is the plugin. It carries the claim lister for need extraction, the
// slice lister for the driver's device order and request policy, and the
// reservations overlay that keeps a burst of claims from all scoring against
// the same not-yet-republished annotation.
type CCXAlign struct {
	handle       fwk.Handle
	claims       claimLister
	slices       sliceLister
	reservations *reservations
}

// claimLister and sliceLister are the two informer reads the plugin does,
// narrowed for the tests.
type claimLister interface {
	get(namespace, name string) (*resourceapi.ResourceClaim, error)
}

type sliceLister interface {
	forNode(nodeName string) []*resourceapi.ResourceSlice
}

var (
	_ fwk.PreFilterPlugin = &CCXAlign{}
	_ fwk.PreScorePlugin  = &CCXAlign{}
	_ fwk.ScorePlugin     = &CCXAlign{}
	_ fwk.ReservePlugin   = &CCXAlign{}
)

// New initializes the plugin.
func New(_ context.Context, _ runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	factory := handle.SharedInformerFactory()
	slices, err := newSliceLister(factory.Resource().V1().ResourceSlices().Informer())
	if err != nil {
		return nil, fmt.Errorf("indexing ResourceSlices by node: %w", err)
	}
	plugin := &CCXAlign{
		handle:       handle,
		claims:       informerClaimLister{lister: factory.Resource().V1().ResourceClaims().Lister()},
		slices:       slices,
		reservations: newReservations(),
	}
	// A reservation bridges the gap between this scheduler allocating a claim
	// and the driver's annotation reflecting it. A claim that dies instead is
	// never reflected, so its reservation is cleared here rather than waiting
	// out the TTL.
	_, err = factory.Resource().V1().ResourceClaims().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			if claim, ok := claimFromDeleteEvent(obj); ok {
				plugin.reservations.removeClaim(claim.UID)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if claim, ok := newObj.(*resourceapi.ResourceClaim); ok && claim.Status.Allocation == nil {
				plugin.reservations.removeClaim(claim.UID)
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("registering the claim event handler: %w", err)
	}
	return plugin, nil
}

func claimFromDeleteEvent(obj interface{}) (*resourceapi.ResourceClaim, bool) {
	if claim, ok := obj.(*resourceapi.ResourceClaim); ok {
		return claim, true
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		claim, ok := tombstone.Obj.(*resourceapi.ResourceClaim)
		return claim, ok
	}
	return nil, false
}

// Name returns the plugin name.
func (p *CCXAlign) Name() string { return Name }

// claimNeed is one unallocated dra.cpu claim's demand.
type claimNeed struct {
	claimUID types.UID
	// device distinguishes the several devices one claim may ask for.
	device int
	cpus   int
}

// alignState carries the pod's demands through the cycle.
type alignState struct {
	needs []claimNeed
}

func (s *alignState) Clone() fwk.StateData { return &alignState{needs: s.needs} }

// PreFilter resolves the pod's dra.cpu claims to one demand per DEVICE they
// ask for: each lands on its own device and alignment is per device. Pods
// without such claims write no state, and PreScore then skips the plugin.
func (p *CCXAlign) PreFilter(_ context.Context, state fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	needs := p.podNeeds(pod)
	if len(needs) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	state.Write(stateKey, &alignState{needs: needs})
	return nil, nil
}

// PreFilterExtensions is nil: the demands do not depend on other pods.
func (p *CCXAlign) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

// podNeeds walks pod.Spec.ResourceClaims -- not only the status, which never
// lists claims referenced directly by name -- and resolves each through the
// same helper the in-tree DRA plugin uses. Its PreEnqueue has already required
// every claim to exist, so a lister miss here is a cold cache, worth a neutral
// score and not a rejected pod.
func (p *CCXAlign) podNeeds(pod *v1.Pod) []claimNeed {
	var needs []claimNeed
	for i := range pod.Spec.ResourceClaims {
		claimName, _, err := resourceclaim.Name(pod, &pod.Spec.ResourceClaims[i])
		if err != nil || claimName == nil {
			continue
		}
		claim, err := p.claims.get(pod.Namespace, *claimName)
		if err != nil {
			continue
		}
		if claim.Status.Allocation != nil {
			// Allocated: the node was chosen, there is nothing to steer.
			continue
		}
		for device, cpus := range claimDeviceCPUs(claim) {
			needs = append(needs, claimNeed{claimUID: claim.UID, device: device, cpus: cpus})
		}
	}
	return needs
}

// claimDeviceCPUs is what the claim asks of each device it will be given, one
// entry per device.
//
// A request's Count is a number of DEVICES, not a multiplier on one: Count 2
// of 16 CPUs is two 16-CPU devices, and under groupBy numanode those are two
// different NUMA nodes. Folding it into a single 32-CPU demand would score one
// NUMA node against a claim it never has to hold. For the same reason every
// request contributes, rather than the largest standing in for all of them.
func claimDeviceCPUs(claim *resourceapi.ResourceClaim) []int {
	var cpus []int
	for _, request := range claim.Spec.Devices.Requests {
		exactly := request.Exactly
		if exactly == nil || exactly.DeviceClassName != deviceClassName || exactly.Capacity == nil {
			continue
		}
		quantity, ok := exactly.Capacity.Requests[capacityName]
		if !ok {
			continue
		}
		per := int(quantity.Value())
		if per <= 0 {
			continue
		}
		count := int(exactly.Count)
		if count < 1 {
			count = 1
		}
		for range count {
			cpus = append(cpus, per)
		}
	}
	return cpus
}

// PreScore skips the whole scoring extension for pods without dra.cpu claims.
// This is the extension point that actually does that: a Skip from PreFilter
// only skips Filter.
func (p *CCXAlign) PreScore(_ context.Context, state fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) *fwk.Status {
	if _, err := state.Read(stateKey); err != nil {
		return fwk.NewStatus(fwk.Skip)
	}
	return nil
}

// Score ranks the node by the shape of its free CPUs as the driver last
// published it, minus what this scheduler has placed against it since. It
// never returns an error: one bad annotation would otherwise reject the pod on
// every node, so anything unreadable scores as unknown instead.
func (p *CCXAlign) Score(ctx context.Context, state fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	data, err := state.Read(stateKey)
	if err != nil {
		// The same "nothing is known about this pod here" the unknown-node
		// paths below report, and scored the same way: this plugin has no
		// ScoreExtensions, so raw scores are compared as they are and a 0
		// among 20s would rank every node it touches last.
		return scoreSplit, nil
	}
	needs := data.(*alignState).needs

	node := nodeInfo.Node()
	raw, ok := node.Annotations[FitAnnotation]
	if !ok {
		return scoreSplit, nil
	}
	report, err := parseFit(raw)
	if err != nil {
		klog.FromContext(ctx).V(4).Info("ignoring unreadable fit annotation", "node", node.Name, "err", err)
		return scoreSplit, nil
	}

	numaOrder, policy := p.deviceOrder(node.Name)
	if len(numaOrder) == 0 {
		return scoreSplit, nil
	}
	if report.Policy != "" {
		policy.placement = report.Policy
	}

	subtractNeeds(report, numaOrder, p.reservations.needsFor(node.Name), policy.placement)

	rounded := make([]int, 0, len(needs))
	for _, need := range needs {
		rounded = append(rounded, policy.round(need.cpus))
	}
	return scoreNode(report, numaOrder, rounded, policy.placement), nil
}

// ScoreExtensions is nil: scores are already on the 0-100 scale.
func (p *CCXAlign) ScoreExtensions() fwk.ScoreExtensions { return nil }

// Reserve notes the pod's claims against the node, so the pods behind it in a
// burst stop scoring the annotation's pre-placement shape. The note dies when
// the claim dies, and after reservationTTL regardless: by then the driver has
// prepared the claim and republished, since preparation precedes even the pod
// sandbox.
func (p *CCXAlign) Reserve(_ context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	data, err := state.Read(stateKey)
	if err != nil {
		return nil
	}
	numaOrder, policy := p.deviceOrder(nodeName)
	if len(numaOrder) == 0 {
		return nil
	}
	for _, need := range data.(*alignState).needs {
		p.reservations.add(nodeName, pod.UID, need.claimUID, need.device, policy.round(need.cpus))
	}
	return nil
}

// Unreserve withdraws the notes when binding fails.
func (p *CCXAlign) Unreserve(_ context.Context, _ fwk.CycleState, pod *v1.Pod, _ string) {
	p.reservations.removePod(pod.UID)
}

// subtractNeeds walks reserved demands largest-first through the same
// first-fit the allocator uses and removes them from both views, so the
// node is scored as if those claims had already landed.
func subtractNeeds(report *fitReport, numaOrder []int, needs []int, policy string) {
	if len(needs) == 0 {
		return
	}
	index := make(map[int]int, len(report.NUMA))
	for i, numa := range report.NUMA {
		index[numa.ID] = i
	}
	for _, need := range needs {
		for _, numaID := range numaOrder {
			i, ok := index[numaID]
			if !ok || sum(report.NUMA[i].FreeCPUs) < need {
				continue
			}
			report.NUMA[i].FreeCPUs = place(report.NUMA[i].CacheCPUs, report.NUMA[i].FreeCPUs, need, policy)
			report.NUMA[i].RepackedFreeCPUs = place(report.NUMA[i].CacheCPUs, report.NUMA[i].RepackedFreeCPUs, need, policy)
			break
		}
	}
}
