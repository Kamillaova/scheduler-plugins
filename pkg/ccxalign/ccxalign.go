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

	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
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
	_ fwk.PreFilterPlugin   = &CCXAlign{}
	_ fwk.FilterPlugin      = &CCXAlign{}
	_ fwk.PreScorePlugin    = &CCXAlign{}
	_ fwk.ScorePlugin       = &CCXAlign{}
	_ fwk.EnqueueExtensions = &CCXAlign{}
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

func (p *CCXAlign) Filter(_ context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	data, err := state.Read(stateKey)
	if err != nil {
		return nil
	}
	alignData := data.(*alignState)
	if len(alignData.claims) == 0 {
		return nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Unschedulable, "node not found")
	}

	for _, claim := range alignData.claims {
		for _, reserved := range claim.Status.ReservedFor {
			if reserved.UID != "" && reserved.UID != pod.UID {
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("claim %q is reserved for pod UID %q, not %q", claim.Name, reserved.UID, pod.UID))
			}
			if reserved.UID == "" && reserved.Name != pod.Name {
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("claim %q is reserved for pod %q, not %q", claim.Name, reserved.Name, pod.Name))
			}
		}
	}

	rawDevices, policy := p.nodeDevices(node.Name)

	var allocatedState *structured.AllocatedState
	if p.claims != nil {
		allocatedState, _ = p.claims.GatherAllocatedState()
	}

	for _, claim := range alignData.claims {
		if claim.Status.Allocation != nil {
			allocatedHere := false
			for _, result := range claim.Status.Allocation.Devices.Results {
				if result.Driver != driverName {
					continue
				}
				for _, d := range rawDevices {
					if d.name == result.Device {
						allocatedHere = true
						break
					}
				}
				if allocatedHere {
					break
				}
			}
			if !allocatedHere {
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("claim %q is allocated on another node", claim.Name))
			}
			continue
		}

		if getClaimAlignment(claim) != v1alpha1.AlignmentRepairable {
			continue
		}

		if len(rawDevices) == 0 {
			failClosedTotal.WithLabelValues("no_devices").Inc()
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q has no dra.cpu devices", node.Name))
		}

		claimPartition := getClaimPartition(claim)

		hasCacheGrouping := false
		partitionDevicesExist := false
		for _, dev := range rawDevices {
			if dev.partition == claimPartition {
				partitionDevicesExist = true
				if dev.hasCacheID {
					hasCacheGrouping = true
				}
			}
		}

		if !partitionDevicesExist {
			failClosedTotal.WithLabelValues("no_partition_devices").Inc()
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q has no devices in partition %q", node.Name, claimPartition))
		}
		if !hasCacheGrouping {
			failClosedTotal.WithLabelValues("coarse_grouping").Inc()
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q does not use cache grouping for partition %q", node.Name, claimPartition))
		}

		om, _ := newOverlayMap(rawDevices, allocatedState, claimPartition)
		cacheSize := om.cacheSize()

		rawDemand := 0
		count := 1
		var firstReq *resourceapi.DeviceRequest
		if len(claim.Spec.Devices.Requests) > 0 {
			firstReq = &claim.Spec.Devices.Requests[0]
			if firstReq.Exactly != nil {
				count = int(firstReq.Exactly.Count)
				if count < 1 {
					count = 1
				}
				if firstReq.Exactly.Capacity != nil {
					if qty, ok := firstReq.Exactly.Capacity.Requests[capacityName]; ok {
						rawDemand = int(qty.Value())
					}
				}
			} else if len(firstReq.FirstAvailable) > 0 {
				count = int(firstReq.FirstAvailable[0].Count)
				if count < 1 {
					count = 1
				}
				if firstReq.FirstAvailable[0].Capacity != nil {
					if qty, ok := firstReq.FirstAvailable[0].Capacity.Requests[capacityName]; ok {
						rawDemand = int(qty.Value())
					}
				}
			}
		}

		roundedDemand := policy.round(rawDemand)
		totalDemand := count * roundedDemand
		k := (totalDemand + cacheSize - 1) / cacheSize
		if k < 1 {
			k = 1
		}

		alignedFitsNow := false
		var alignedLandingNUMA int
		for _, numaID := range om.numaOrder() {
			if om.cleanCachesInNUMA(numaID, cacheSize) >= k {
				alignedLandingNUMA = numaID
				alignedFitsNow = true
				break
			}
		}

		if alignedFitsNow {
			cachesDebited := 0
			for _, dev := range om.byNUMA[alignedLandingNUMA] {
				if dev.freeCPUs >= cacheSize && dev.consumedCPUs == 0 {
					dev.consumedCPUs = dev.totalCPUs
					dev.freeCPUs = 0
					cachesDebited++
					if cachesDebited == k {
						break
					}
				}
			}
			alignData.recordInFlightLanding(node.Name, alignedLandingNUMA)
			continue
		}

		shape := inferClaimShape(claim)
		if shape != shapeFlexible || firstReq == nil || len(firstReq.FirstAvailable) <= 1 {
			failClosedTotal.WithLabelValues("no_split_alternatives").Inc()
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("claim %q does not fit aligned and has no split alternatives", claim.Name))
		}

		var feasibleNUMAs []int
		splitDemand := 0
		for _, splitAlt := range firstReq.FirstAvailable[1:] {
			subCount := int(splitAlt.Count)
			if subCount < 1 {
				subCount = 1
			}
			subRaw := 0
			if splitAlt.Capacity != nil {
				if qty, ok := splitAlt.Capacity.Requests[capacityName]; ok {
					subRaw = int(qty.Value())
				}
			}
			subCPUs := policy.round(subRaw)
			curDemand := subCount * subCPUs

			var numas []int
			for _, numaID := range om.numaOrder() {
				if om.freeCPUsInNUMA(numaID) >= curDemand {
					numas = append(numas, numaID)
				}
			}
			if len(numas) > 0 {
				feasibleNUMAs = numas
				splitDemand = curDemand
				break
			}
		}

		if len(feasibleNUMAs) == 0 {
			failClosedTotal.WithLabelValues("no_feasible_numa").Inc()
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("claim %q split alternatives do not fit on any NUMA node of node %q", claim.Name, node.Name))
		}

		for _, numaID := range feasibleNUMAs {
			if alignData.hasInFlightLanding(node.Name, numaID) {
				failClosedTotal.WithLabelValues("in_flight_invalidation").Inc()
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("NUMA node %d on node %q has in-flight allocation in this cycle", numaID, node.Name))
			}

			repairRaw := ""
			for _, d := range om.byNUMA[numaID] {
				if d.repairRounds != "" {
					repairRaw = d.repairRounds
					break
				}
			}
			rounds := parseRepairRounds(repairRaw, k)
			if rounds < 1 || rounds > 3 {
				failClosedTotal.WithLabelValues("unreachable_frontier").Inc()
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("NUMA node %d on node %q has unreachable repair frontier %q for size %d", numaID, node.Name, repairRaw, k))
			}

			publishedDigest := ""
			for _, d := range om.byNUMA[numaID] {
				if d.frontierInput != "" {
					publishedDigest = d.frontierInput
					break
				}
			}
			if publishedDigest == "" {
				failClosedTotal.WithLabelValues("missing_frontier_input").Inc()
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("NUMA node %d on node %q missing frontier input digest", numaID, node.Name))
			}

			expectedDigest := p.numaAllocatedDigest(node.Name, numaID, claimPartition, rawDevices)
			if publishedDigest != expectedDigest {
				failClosedTotal.WithLabelValues("stale_frontier_digest").Inc()
				return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("NUMA node %d on node %q frontier digest mismatch: published %q != expected %q", numaID, node.Name, publishedDigest, expectedDigest))
			}
		}

		landingNUMA := feasibleNUMAs[0]
		rem := splitDemand
		for _, dev := range om.byNUMA[landingNUMA] {
			if dev.freeCPUs > 0 {
				debit := min(dev.freeCPUs, rem)
				dev.freeCPUs -= debit
				dev.consumedCPUs += debit
				rem -= debit
				if rem == 0 {
					break
				}
			}
		}
		alignData.recordInFlightLanding(node.Name, landingNUMA)
	}

	return nil
}

func (p *CCXAlign) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceSlice, ActionType: fwk.Add | fwk.Update}},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.Update}},
	}, nil
}

func getClaimAlignment(claim *resourceapi.ResourceClaim) v1alpha1.Alignment {
	for _, cfg := range claim.Spec.Devices.Config {
		if cfg.Opaque != nil && cfg.Opaque.Driver == driverName {
			parsed, err := api.ParseOpaqueConfig(cfg.Opaque.Parameters.Raw)
			if err == nil && parsed.Alignment != "" {
				return parsed.Alignment
			}
		}
	}
	return v1alpha1.AlignmentBestEffort
}

func (p *CCXAlign) numaAllocatedDigest(nodeName string, numaID int, partition string, rawDevices []deviceInfo) string {
	if p.claims == nil {
		return api.FrontierInputDigest([]string{})
	}
	allClaims, err := p.claims.List()
	if err != nil {
		return ""
	}
	devMap := make(map[string]bool)
	for _, d := range rawDevices {
		if d.partition == partition && d.numaNodeID == numaID && d.hasCacheID {
			devMap[d.poolName+"/"+d.name] = true
		}
	}
	uids := make(map[string]bool)
	for _, claim := range allClaims {
		if claim.Status.Allocation == nil {
			continue
		}
		for _, res := range claim.Status.Allocation.Devices.Results {
			if res.Driver == driverName && devMap[res.Pool+"/"+res.Device] {
				uids[string(claim.UID)] = true
				break
			}
		}
	}
	uidList := make([]string, 0, len(uids))
	for uid := range uids {
		uidList = append(uidList, uid)
	}
	return api.FrontierInputDigest(uidList)
}

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
