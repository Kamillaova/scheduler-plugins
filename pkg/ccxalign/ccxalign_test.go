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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/utils/ptr"
)

type fakeClaimTracker struct {
	claims         map[string]*resourceapi.ResourceClaim
	allocatedState *structured.AllocatedState
}

func (f *fakeClaimTracker) Get(namespace, name string) (*resourceapi.ResourceClaim, error) {
	claim, ok := f.claims[namespace+"/"+name]
	if !ok {
		return nil, fmt.Errorf("claim %s/%s not found", namespace, name)
	}
	return claim, nil
}

func (f *fakeClaimTracker) GatherAllocatedState() (*structured.AllocatedState, error) {
	if f.allocatedState == nil {
		return &structured.AllocatedState{
			AllocatedDevices:   sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.NewConsumedCapacityCollection(),
		}, nil
	}
	return f.allocatedState, nil
}

func (f *fakeClaimTracker) List() ([]*resourceapi.ResourceClaim, error) {
	var list []*resourceapi.ResourceClaim
	for _, c := range f.claims {
		list = append(list, c)
	}
	return list, nil
}

type fakeClassLister map[string]*resourceapi.DeviceClass

func (f fakeClassLister) Get(name string) (*resourceapi.DeviceClass, error) {
	class, ok := f[name]
	if !ok {
		return nil, fmt.Errorf("device class %q not found", name)
	}
	return class, nil
}

type fakeSlices map[string][]*resourceapi.ResourceSlice

func (f fakeSlices) forNode(nodeName string) []*resourceapi.ResourceSlice {
	return f[nodeName]
}

func makeTestClaim(name string, uid types.UID, cpus int, className string, partition string) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: uid},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "cpus",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: className,
						Capacity: &resourceapi.CapacityRequirements{
							Requests: map[resourceapi.QualifiedName]resource.Quantity{
								capacityName: *resource.NewQuantity(int64(cpus), resource.DecimalSI),
							},
						},
					},
				}},
			},
		},
	}
	if partition != "" {
		claim.Spec.Devices.Requests[0].Exactly.Tolerations = []resourceapi.DeviceToleration{{
			Key:   partitionAttr,
			Value: partition,
		}}
	}
	return claim
}

func makeTestFlexibleClaim(name string, uid types.UID, totalCPUs int, className string, partition string) *resourceapi.ResourceClaim {
	k := totalCPUs / 16
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: uid},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "cpus",
					FirstAvailable: []resourceapi.DeviceSubRequest{
						{
							Name:            "aligned",
							DeviceClassName: className,
							Count:           int64(k),
							Capacity: &resourceapi.CapacityRequirements{
								Requests: map[resourceapi.QualifiedName]resource.Quantity{
									capacityName: *resource.NewQuantity(16, resource.DecimalSI),
								},
							},
						},
						{
							Name:            "split",
							DeviceClassName: className,
							Count:           int64(k * 2),
							Capacity: &resourceapi.CapacityRequirements{
								Requests: map[resourceapi.QualifiedName]resource.Quantity{
									capacityName: *resource.NewQuantity(8, resource.DecimalSI),
								},
							},
						},
					},
				}},
			},
		},
	}
	if partition != "" {
		for i := range claim.Spec.Devices.Requests[0].FirstAvailable {
			claim.Spec.Devices.Requests[0].FirstAvailable[i].Tolerations = []resourceapi.DeviceToleration{{
				Key:   partitionAttr,
				Value: partition,
			}}
		}
	}
	return claim
}

func makeCacheDevice(name string, numa int64, partition string, capacity int, repairRounds string) resourceapi.Device {
	return makeCacheDeviceWithFrontier(name, numa, partition, capacity, repairRounds, "")
}

func makeCacheDeviceWithFrontier(name string, numa int64, partition string, capacity int, repairRounds string, frontierInput string) resourceapi.Device {
	var cacheIndex int64
	fmt.Sscanf(name, "cache-%d", &cacheIndex)
	dev := resourceapi.Device{
		Name: name,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			numaAttribute: {IntValue: &numa},
			cacheL3IDAttr: {IntValue: &cacheIndex},
		},
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			capacityName: {
				Value: *resource.NewQuantity(int64(capacity), resource.DecimalSI),
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default: ptr.To(resource.MustParse(fmt.Sprintf("%d", capacity))),
					ValidRange: &resourceapi.CapacityRequestPolicyRange{
						Min:  ptr.To(resource.MustParse("2")),
						Step: ptr.To(resource.MustParse("2")),
					},
				},
			},
		},
	}
	if partition != "" {
		dev.Attributes[partitionAttr] = resourceapi.DeviceAttribute{StringValue: &partition}
	}
	if repairRounds != "" {
		dev.Attributes[repairRoundsAttr] = resourceapi.DeviceAttribute{StringValue: &repairRounds}
	}
	if frontierInput != "" {
		dev.Attributes[frontierInputAttr] = resourceapi.DeviceAttribute{StringValue: &frontierInput}
	}
	return dev
}

func makeCoarseDevice(name string, numa int64, partition string, capacity int) resourceapi.Device {
	dev := resourceapi.Device{
		Name: name,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			numaAttribute: {IntValue: &numa},
		},
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			capacityName: {
				Value: *resource.NewQuantity(int64(capacity), resource.DecimalSI),
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default: ptr.To(resource.MustParse(fmt.Sprintf("%d", capacity))),
					ValidRange: &resourceapi.CapacityRequestPolicyRange{
						Min:  ptr.To(resource.MustParse("2")),
						Step: ptr.To(resource.MustParse("2")),
					},
				},
			},
		},
	}
	if partition != "" {
		dev.Attributes[partitionAttr] = resourceapi.DeviceAttribute{StringValue: &partition}
	}
	return dev
}

func withRepairable(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	cfg := v1alpha1.OpaqueConfig{
		APIVersion: v1alpha1.APIVersion,
		CPUConfig: v1alpha1.CPUConfig{
			Relocatable: true,
			Alignment:   v1alpha1.AlignmentRepairable,
		},
	}
	raw, _ := json.Marshal(cfg)
	claim.Spec.Devices.Config = []resourceapi.DeviceClaimConfiguration{{
		DeviceConfiguration: resourceapi.DeviceConfiguration{
			Opaque: &resourceapi.OpaqueDeviceConfiguration{
				Driver:     driverName,
				Parameters: runtime.RawExtension{Raw: raw},
			},
		},
	}}
	return claim
}

func makeNodeSlice(nodeName string, devices ...resourceapi.Device) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-slice"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driverName,
			NodeName: ptr.To(nodeName),
			Pool:     resourceapi.ResourcePool{Name: "cpu-pool"},
			Devices:  devices,
		},
	}
}

func testPod(uid types.UID, claimNames ...string) *v1.Pod {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-" + string(uid), UID: uid}}
	for _, name := range claimNames {
		pod.Spec.ResourceClaims = append(pod.Spec.ResourceClaims, v1.PodResourceClaim{
			Name:              name,
			ResourceClaimName: ptr.To(name),
		})
	}
	return pod
}

func nodeInfoOf(node *v1.Node) *framework.NodeInfo {
	info := framework.NewNodeInfo()
	info.SetNode(node)
	return info
}

func setupTestPlugin(tracker *fakeClaimTracker, classes fakeClassLister, slices fakeSlices, args *CCXAlignArgs) *CCXAlign {
	if args == nil {
		args = &CCXAlignArgs{ScoringStrategy: &ScoringStrategy{Type: MostAllocated}}
	}
	return &CCXAlign{
		args:    args,
		claims:  tracker,
		classes: classes,
		slices:  slices,
	}
}

func standardDRADeviceClass() *resourceapi.DeviceClass {
	return &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "dra.cpu"},
		Spec: resourceapi.DeviceClassSpec{
			Config: []resourceapi.DeviceClassConfiguration{
				{
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: driverName},
					},
				},
			},
		},
	}
}

func otherDriverDeviceClass() *resourceapi.DeviceClass {
	return &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu.class"},
		Spec: resourceapi.DeviceClassSpec{
			Config: []resourceapi.DeviceClassConfiguration{
				{
					DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: "dra.gpu"},
					},
				},
			},
		},
	}
}

func TestPreFilterResolvesClassAndSkipsNonDriver(t *testing.T) {
	classes := fakeClassLister{
		"dra.cpu":   standardDRADeviceClass(),
		"gpu.class": otherDriverDeviceClass(),
	}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/cpu-claim": makeTestClaim("cpu-claim", "uid-cpu", 4, "dra.cpu", ""),
			"ns/gpu-claim": makeTestClaim("gpu-claim", "uid-gpu", 1, "gpu.class", ""),
		},
	}
	slices := fakeSlices{"node-1": {makeNodeSlice("node-1", makeCacheDevice("cache-0", 0, "", 16, ""))}}
	plugin := setupTestPlugin(tracker, classes, slices, nil)

	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, testPod("p-none"), nil)
	if status == nil || status.Code() != fwk.Skip {
		t.Fatalf("want Skip for pod with no claims, got %v", status)
	}

	stateGPU := framework.NewCycleState()
	_, statusGPU := plugin.PreFilter(context.Background(), stateGPU, testPod("p-gpu", "gpu-claim"), nil)
	if statusGPU == nil || statusGPU.Code() != fwk.Skip {
		t.Fatalf("want Skip for non-dra.cpu claim, got %v", statusGPU)
	}

	stateCPU := framework.NewCycleState()
	_, statusCPU := plugin.PreFilter(context.Background(), stateCPU, testPod("p-cpu", "cpu-claim"), nil)
	if statusCPU != nil && !statusCPU.IsSuccess() {
		t.Fatalf("want Success for dra.cpu claim, got %v", statusCPU)
	}
	if got := plugin.PreScore(context.Background(), stateCPU, testPod("p-cpu", "cpu-claim"), nil); got != nil && !got.IsSuccess() {
		t.Fatalf("PreScore should succeed when state present, got %v", got)
	}
}

func TestAbsoluteCodebookScoringPackingAndSpreading(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-aligned":  makeTestClaim("claim-aligned", "uid-aligned", 16, "dra.cpu", ""),
			"ns/claim-repair-1": makeTestFlexibleClaim("claim-repair-1", "uid-r1", 16, "dra.cpu", ""),
		},
	}

	nodeCold := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-cold"}}
	slicesCold := fakeSlices{
		"node-cold": {
			makeNodeSlice("node-cold",
				makeCacheDevice("c0", 0, "default", 16, "1,-,-,-"),
				makeCacheDevice("c1", 0, "default", 16, "1,-,-,-"),
			),
		},
	}

	nodeWarm := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-warm"}}
	slicesWarm := fakeSlices{
		"node-warm": {
			makeNodeSlice("node-warm",
				makeCacheDevice("c0", 0, "default", 16, "1,-,-,-"),
				makeCacheDevice("c1", 0, "default", 16, "1,-,-,-"),
			),
		},
	}
	warmAllocated := &structured.AllocatedState{
		AllocatedDevices: sets.New[structured.DeviceID](),
		AggregatedCapacity: structured.ConsumedCapacityCollection{
			structured.MakeDeviceID(driverName, "cpu-pool", "c1"): structured.ConsumedCapacity{
				capacityName: resource.NewQuantity(8, resource.DecimalSI),
			},
		},
	}

	packingPlugin := setupTestPlugin(tracker, classes, slicesCold, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	})
	podAligned := testPod("p1", "claim-aligned")
	state1 := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, packingPlugin, state1, podAligned})

	scoreCold, _ := packingPlugin.Score(context.Background(), state1, podAligned, nodeInfoOf(nodeCold))
	if scoreCold < 40 || scoreCold > 48 {
		t.Fatalf("Cold-aligned in packing should be in band 40..48, got %d", scoreCold)
	}

	trackerWarm := &fakeClaimTracker{
		claims:         tracker.claims,
		allocatedState: warmAllocated,
	}
	packingWarmPlugin := setupTestPlugin(trackerWarm, classes, slicesWarm, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	})
	scoreWarm, _ := packingWarmPlugin.Score(context.Background(), state1, podAligned, nodeInfoOf(nodeWarm))
	if scoreWarm < 80 || scoreWarm > 88 {
		t.Fatalf("Warm-aligned in packing should be in band 80..88, got %d", scoreWarm)
	}

	spreadingPlugin := setupTestPlugin(tracker, classes, slicesCold, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: LeastAllocated},
	})
	stateSpread := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, spreadingPlugin, stateSpread, podAligned})
	scoreSpread, _ := spreadingPlugin.Score(context.Background(), stateSpread, podAligned, nodeInfoOf(nodeCold))
	if scoreSpread < 80 || scoreSpread > 88 {
		t.Fatalf("Spreading mode on cold node should have no cold band (band 80..88), got %d", scoreSpread)
	}
}

func TestRepairableRoundsAndPenalties(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-flex": makeTestFlexibleClaim("claim-flex", "uid-f1", 16, "dra.cpu", ""),
		},
	}

	makeRepairNode := func(name string, rounds string) (*v1.Node, fakeSlices, *structured.AllocatedState) {
		node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		slices := fakeSlices{
			name: {
				makeNodeSlice(name,
					makeCacheDevice("c0", 0, "default", 16, rounds),
					makeCacheDevice("c1", 0, "default", 16, rounds),
				),
			},
		}
		alloc := &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "c0"): structured.ConsumedCapacity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "c1"): structured.ConsumedCapacity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		}
		return node, slices, alloc
	}

	nodeR1, slicesR1, allocR1 := makeRepairNode("node-r1", "1,-,-,-")
	p1 := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocR1}, classes, slicesR1, nil)
	pod := testPod("p1", "claim-flex")
	state1 := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, p1, state1, pod})
	scoreR1, _ := p1.Score(context.Background(), state1, pod, nodeInfoOf(nodeR1))
	if scoreR1 < 60 || scoreR1 > 68 {
		t.Fatalf("1-round repairable should score in 60..68, got %d", scoreR1)
	}

	nodeR2, slicesR2, allocR2 := makeRepairNode("node-r2", "2,-,-,-")
	p2 := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocR2}, classes, slicesR2, nil)
	scoreR2, _ := p2.Score(context.Background(), state1, pod, nodeInfoOf(nodeR2))
	if scoreR2 < 40 || scoreR2 > 48 {
		t.Fatalf("2-round repairable should score in 40..48, got %d", scoreR2)
	}

	nodeR3, slicesR3, allocR3 := makeRepairNode("node-r3", "3,-,-,-")
	p3 := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocR3}, classes, slicesR3, nil)
	scoreR3, _ := p3.Score(context.Background(), state1, pod, nodeInfoOf(nodeR3))
	if scoreR3 < 20 || scoreR3 > 28 {
		t.Fatalf("3-round repairable should score in 20..28, got %d", scoreR3)
	}

	nodeUnreach, slicesUnreach, allocUnreach := makeRepairNode("node-unreach", "-,-,-,-")
	pUnreach := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocUnreach}, classes, slicesUnreach, nil)
	scoreUnreach, _ := pUnreach.Score(context.Background(), state1, pod, nodeInfoOf(nodeUnreach))
	if scoreUnreach < 0 || scoreUnreach > 8 {
		t.Fatalf("Unreachable repair should score in split band 0..8, got %d", scoreUnreach)
	}
}

func TestSubCacheFootprintBonus(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/sub-claim": makeTestClaim("sub-claim", "uid-sub", 4, "dra.cpu", ""),
		},
	}

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("c0", 0, "default", 16, ""),
				makeCacheDevice("c1", 0, "default", 16, ""),
			),
		},
	}

	allocOccupied := &structured.AllocatedState{
		AllocatedDevices: sets.New[structured.DeviceID](),
		AggregatedCapacity: structured.ConsumedCapacityCollection{
			structured.MakeDeviceID(driverName, "cpu-pool", "c0"): structured.ConsumedCapacity{
				capacityName: resource.NewQuantity(4, resource.DecimalSI),
			},
		},
	}
	pOccupied := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocOccupied}, classes, slices, nil)
	pod := testPod("p1", "sub-claim")
	stateOcc := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, pOccupied, stateOcc, pod})
	scoreOcc, _ := pOccupied.Score(context.Background(), stateOcc, pod, nodeInfoOf(node))
	if scoreOcc != 88 {
		t.Fatalf("Reusing occupied cache under packing should get bonus 8 (80+8=88), got %d", scoreOcc)
	}

	allocEmpty := &structured.AllocatedState{
		AllocatedDevices: sets.New[structured.DeviceID](),
		AggregatedCapacity: structured.ConsumedCapacityCollection{
			structured.MakeDeviceID(driverName, "cpu-pool", "c1"): structured.ConsumedCapacity{
				capacityName: resource.NewQuantity(4, resource.DecimalSI),
			},
		},
	}
	pClean := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocEmpty}, classes, slices, nil)
	stateClean := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, pClean, stateClean, pod})
	scoreClean, _ := pClean.Score(context.Background(), stateClean, pod, nodeInfoOf(node))
	if scoreClean != 80 {
		t.Fatalf("Opening clean cache under packing should get bonus 0 (80+0=80), got %d", scoreClean)
	}
}

func TestPartitionScoping(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/vm-claim": makeTestClaim("vm-claim", "uid-vm", 16, "dra.cpu", "vm"),
		},
	}

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("dp-0", 0, "dataplane", 16, ""),
				makeCacheDevice("vm-0", 0, "vm", 16, ""),
			),
		},
	}

	allocDataplane := &structured.AllocatedState{
		AllocatedDevices: sets.New[structured.DeviceID](),
		AggregatedCapacity: structured.ConsumedCapacityCollection{
			structured.MakeDeviceID(driverName, "cpu-pool", "dp-0"): structured.ConsumedCapacity{
				capacityName: resource.NewQuantity(16, resource.DecimalSI),
			},
		},
	}

	plugin := setupTestPlugin(&fakeClaimTracker{claims: tracker.claims, allocatedState: allocDataplane}, classes, slices, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	})
	pod := testPod("p-vm", "vm-claim")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	score, _ := plugin.Score(context.Background(), state, pod, nodeInfoOf(node))
	if score < 40 || score > 48 {
		t.Fatalf("Dataplane usage should leave VM partition cold (band 40..48), got %d", score)
	}
}

func TestMultiClaimJointOverlay(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": makeTestClaim("claim-1", "uid-1", 16, "dra.cpu", "default"),
			"ns/claim-2": makeTestFlexibleClaim("claim-2", "uid-2", 16, "dra.cpu", "default"),
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "c1"): structured.ConsumedCapacity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "c2"): structured.ConsumedCapacity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("c0", 0, "default", 16, "1,-,-,-"),
				makeCacheDevice("c1", 0, "default", 16, "1,-,-,-"),
				makeCacheDevice("c2", 0, "default", 16, "1,-,-,-"),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	})
	pod := testPod("p-multi", "claim-1", "claim-2")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	score, _ := plugin.Score(context.Background(), state, pod, nodeInfoOf(node))
	if score < 60 || score > 68 {
		t.Fatalf("Multi-claim joint overlay: claim 1 takes c0 (aligned 80), claim 2 splits across c1 and c2 (repairable round 1 60..68), got %d", score)
	}

	data, _ := state.Read(stateKey)
	numa, ok := data.(*alignState).getPredictedNUMA("worker")
	if !ok || numa != 0 {
		t.Fatalf("audit figure: want predicted landing NUMA 0, got %d (ok=%v)", numa, ok)
	}
}

func TestAllocatedClaimScoring(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	allocatedClaim := makeTestClaim("claim-alloc", "uid-alloc", 16, "dra.cpu", "default")
	allocatedClaim.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver: driverName,
					Pool:   "cpu-pool",
					Device: "c0",
				},
			},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-alloc": allocatedClaim,
		},
	}

	nodeWorker := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	slicesWorker := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("c0", 0, "default", 16, ""),
				makeCacheDevice("c1", 0, "default", 16, ""),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slicesWorker, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: MostAllocated},
	})
	pod := testPod("p-alloc", "claim-alloc")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	scoreOnPinnedNode, _ := plugin.Score(context.Background(), state, pod, nodeInfoOf(nodeWorker))
	if scoreOnPinnedNode < 40 || scoreOnPinnedNode > 48 {
		t.Fatalf("Allocated claim on cold pinned node should score cold-aligned (40..48), got %d", scoreOnPinnedNode)
	}

	nodeOther := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other"}}
	slicesOther := fakeSlices{
		"other": {
			makeNodeSlice("other",
				makeCacheDevice("other-c0", 0, "default", 16, ""),
			),
		},
	}
	pluginOther := setupTestPlugin(tracker, classes, slicesOther, nil)
	scoreOnOtherNode, _ := pluginOther.Score(context.Background(), state, pod, nodeInfoOf(nodeOther))
	if scoreOnOtherNode != 0 {
		t.Fatalf("Allocated claim on non-pinned node should score 0, got %d", scoreOnOtherNode)
	}
}

func TestSpreadingCleanCacheBonus(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": makeTestClaim("claim-1", "uid-1", 16, "dra.cpu", "default"),
		},
	}

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("c0", 0, "default", 16, ""),
				makeCacheDevice("c1", 0, "default", 16, ""),
				makeCacheDevice("c2", 0, "default", 16, ""),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, &CCXAlignArgs{
		ScoringStrategy: &ScoringStrategy{Type: LeastAllocated},
	})
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	score, _ := plugin.Score(context.Background(), state, pod, nodeInfoOf(node))
	// 3 clean caches in NUMA 0 -> bonus 3. Base score 80 -> 83.
	if score != 83 {
		t.Fatalf("Spreading mode with 3 clean caches should score 80 + 3 = 83, got %d", score)
	}
}

type pluginPreFilterArgs struct {
	t      *testing.T
	plugin *CCXAlign
	state  fwk.CycleState
	pod    *v1.Pod
}

func pluginPreFilter(args pluginPreFilterArgs) {
	args.t.Helper()
	_, status := args.plugin.PreFilter(context.Background(), args.state, args.pod, nil)
	if status != nil && !status.IsSuccess() {
		args.t.Fatal(status.AsError())
	}
}

func TestFilter_SkipClaimlessPod(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{claims: map[string]*resourceapi.ResourceClaim{}}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker", makeCacheDevice("c0", 0, "default", 16, "")),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1")
	state := framework.NewCycleState()

	_, preStatus := plugin.PreFilter(context.Background(), state, pod, nil)
	if preStatus.Code() != fwk.Skip {
		t.Fatalf("PreFilter for claimless pod should return Skip, got %v", preStatus)
	}

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status != nil && !status.IsSuccess() {
		t.Fatalf("Filter for claimless pod should pass, got %v", status)
	}
}

func TestFilter_UnallocatedBestEffortPasses(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": makeTestClaim("claim-1", "uid-1", 16, "dra.cpu", "default"),
		},
	}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker", makeCacheDevice("c0", 0, "default", 16, "")),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status != nil && !status.IsSuccess() {
		t.Fatalf("Filter for unallocated BestEffort claim should pass, got %v", status)
	}
}

func TestFilter_AllocatedClaimPinnedNode(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	allocClaim := makeTestClaim("claim-alloc", "uid-alloc", 16, "dra.cpu", "default")
	allocClaim.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver: driverName,
					Pool:   "cpu-pool",
					Device: "c0",
				},
			},
		},
	}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-alloc": allocClaim,
		},
	}
	slices := fakeSlices{
		"worker-1": {
			makeNodeSlice("worker-1", makeCacheDevice("c0", 0, "default", 16, "")),
		},
		"worker-2": {
			makeNodeSlice("worker-2", makeCacheDevice("c1", 0, "default", 16, "")),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-alloc")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	status1 := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node1))
	if status1 != nil && !status1.IsSuccess() {
		t.Fatalf("Filter for allocated claim on pinned node should pass, got %v", status1)
	}

	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-2"}}
	status2 := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node2))
	if status2 == nil || status2.Code() != fwk.Unschedulable {
		t.Fatalf("Filter for allocated claim on different node should be Unschedulable, got %v", status2)
	}
}

func TestFilter_MismatchedReservationRejected(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := makeTestClaim("claim-1", "uid-1", 16, "dra.cpu", "default")
	claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{
			Resource: "pods",
			Name:     "other-pod",
			UID:      "other-uid",
		},
	}
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": claim,
		},
	}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker", makeCacheDevice("c0", 0, "default", 16, "")),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("Filter for claim reserved for another pod should be Unschedulable, got %v", status)
	}

	claim.Status.ReservedFor[0].UID = pod.UID
	claim.Status.ReservedFor[0].Name = pod.Name
	statusMatch := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if statusMatch != nil && !statusMatch.IsSuccess() {
		t.Fatalf("Filter for claim reserved for target pod should pass, got %v", statusMatch)
	}
}

func TestFilter_CoarseGroupingRejected(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": claim,
		},
	}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker", makeCoarseDevice("coarse-0", 0, "default", 32)),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("Filter for Repairable claim on coarse grouping node should fail closed with Unschedulable, got %v", status)
	}
}

func TestFilter_AlignedFitsNowPassesWithoutFrontier(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))
	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1": claim,
		},
	}
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDevice("cache-0", 0, "default", 16, ""),
			),
		},
	}
	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status != nil && !status.IsSuccess() {
		t.Fatalf("Filter when aligned alternative fits now should pass without checking frontier, got %v", status)
	}
}

func TestFilter_SplitFeasibleAllReachableFreshPasses(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	digest := api.FrontierInputDigest([]string{"uid-c0", "uid-c1"})
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", digest),
				makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", digest),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status != nil && !status.IsSuccess() {
		t.Fatalf("Filter when all feasible NUMAs are reachable and fresh should pass, got %v", status)
	}
}

func TestFilter_SplitFeasibleOneUnreachableFails(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}
	c2Existing := makeTestClaim("c2-exist", "uid-c2", 8, "dra.cpu", "default")
	c2Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-2"}},
		},
	}
	c3Existing := makeTestClaim("c3-exist", "uid-c3", 8, "dra.cpu", "default")
	c3Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-3"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
			"ns/c2-exist": c2Existing,
			"ns/c3-exist": c3Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-2"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-3"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	digest0 := api.FrontierInputDigest([]string{"uid-c0", "uid-c1"})
	digest1 := api.FrontierInputDigest([]string{"uid-c2", "uid-c3"})

	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", digest0),
				makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", digest0),
				makeCacheDeviceWithFrontier("cache-2", 1, "default", 16, "-,-,-,-", digest1),
				makeCacheDeviceWithFrontier("cache-3", 1, "default", 16, "-,-,-,-", digest1),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("Filter when one feasible NUMA has unreachable frontier should fail closed with Unschedulable, got %v", status)
	}
}

func TestFilter_SplitFeasibleStaleDigestFails(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", "stale-digest-value"),
				makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", "stale-digest-value"),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("Filter with stale digest should fail closed with Unschedulable, got %v", status)
	}
}

func TestFilter_SplitFeasibleInFlightInvalidationFails(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	digest := api.FrontierInputDigest([]string{"uid-c0", "uid-c1"})
	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", digest),
				makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", digest),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	data, _ := state.Read(stateKey)
	alignData := data.(*alignState)
	alignData.recordInFlightLanding("worker", 0)

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("Filter when in-flight allocation landed in NUMA node should fail closed with Unschedulable, got %v", status)
	}
}

func TestFilter_PredecessorRepairMovesFallback(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	digest := api.FrontierInputDigest([]string{"uid-c0", "uid-c1"})
	dev0 := makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "", digest)
	dev1 := makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "", digest)
	movesVal := "2,-,-,-"
	dev0.Attributes[repairMovesAttr] = resourceapi.DeviceAttribute{StringValue: &movesVal}
	dev1.Attributes[repairMovesAttr] = resourceapi.DeviceAttribute{StringValue: &movesVal}

	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker", dev0, dev1),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	status := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if status != nil && !status.IsSuccess() {
		t.Fatalf("Filter with predecessor repairMoves attribute should pass when reachable and fresh, got %v", status)
	}
}

func TestEnqueueExtensions_ResourceSliceRequeue(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	tracker := &fakeClaimTracker{claims: map[string]*resourceapi.ResourceClaim{}}
	slices := fakeSlices{}
	plugin := setupTestPlugin(tracker, classes, slices, nil)

	events, err := plugin.EventsToRegister(context.Background())
	if err != nil {
		t.Fatalf("EventsToRegister returned error: %v", err)
	}
	hasSliceEvent := false
	hasNodeEvent := false
	for _, e := range events {
		if e.Event.Resource == fwk.ResourceSlice && (e.Event.ActionType&(fwk.Add|fwk.Update)) != 0 {
			hasSliceEvent = true
		}
		if e.Event.Resource == fwk.Node && (e.Event.ActionType&(fwk.Add|fwk.Update)) != 0 {
			hasNodeEvent = true
		}
	}
	if !hasSliceEvent {
		t.Fatalf("EventsToRegister must include ResourceSlice Add|Update, got %v", events)
	}
	if !hasNodeEvent {
		t.Fatalf("EventsToRegister must include Node Add|Update, got %v", events)
	}
}

func TestFilter_RequeueOnSliceUpdate(t *testing.T) {
	classes := fakeClassLister{"dra.cpu": standardDRADeviceClass()}
	claim := withRepairable(makeTestFlexibleClaim("claim-1", "uid-1", 16, "dra.cpu", "default"))

	c0Existing := makeTestClaim("c0-exist", "uid-c0", 8, "dra.cpu", "default")
	c0Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-0"}},
		},
	}
	c1Existing := makeTestClaim("c1-exist", "uid-c1", 8, "dra.cpu", "default")
	c1Existing.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{Driver: driverName, Pool: "cpu-pool", Device: "cache-1"}},
		},
	}

	tracker := &fakeClaimTracker{
		claims: map[string]*resourceapi.ResourceClaim{
			"ns/claim-1":  claim,
			"ns/c0-exist": c0Existing,
			"ns/c1-exist": c1Existing,
		},
		allocatedState: &structured.AllocatedState{
			AllocatedDevices: sets.New[structured.DeviceID](),
			AggregatedCapacity: structured.ConsumedCapacityCollection{
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-0"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
				structured.MakeDeviceID(driverName, "cpu-pool", "cache-1"): map[resourceapi.QualifiedName]*resource.Quantity{
					capacityName: resource.NewQuantity(8, resource.DecimalSI),
				},
			},
		},
	}

	slices := fakeSlices{
		"worker": {
			makeNodeSlice("worker",
				makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", "stale-digest"),
				makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", "stale-digest"),
			),
		},
	}

	plugin := setupTestPlugin(tracker, classes, slices, nil)
	pod := testPod("p1", "claim-1")
	state := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state, pod})

	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
	statusStale := plugin.Filter(context.Background(), state, pod, nodeInfoOf(node))
	if statusStale == nil || statusStale.Code() != fwk.Unschedulable {
		t.Fatalf("Initial filter attempt with stale digest should fail, got %v", statusStale)
	}

	freshDigest := api.FrontierInputDigest([]string{"uid-c0", "uid-c1"})
	slices["worker"] = []*resourceapi.ResourceSlice{
		makeNodeSlice("worker",
			makeCacheDeviceWithFrontier("cache-0", 0, "default", 16, "1,-,-,-", freshDigest),
			makeCacheDeviceWithFrontier("cache-1", 0, "default", 16, "1,-,-,-", freshDigest),
		),
	}

	state2 := framework.NewCycleState()
	pluginPreFilter(pluginPreFilterArgs{t, plugin, state2, pod})
	statusFresh := plugin.Filter(context.Background(), state2, pod, nodeInfoOf(node))
	if statusFresh != nil && !statusFresh.IsSuccess() {
		t.Fatalf("Filter retry after slice update should pass, got %v", statusFresh)
	}
}
