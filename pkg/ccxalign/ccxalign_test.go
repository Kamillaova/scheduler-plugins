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
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/utils/ptr"
)

type fakeClaims map[string]*resourceapi.ResourceClaim

func (f fakeClaims) get(namespace, name string) (*resourceapi.ResourceClaim, error) {
	claim, ok := f[namespace+"/"+name]
	if !ok {
		return nil, fmt.Errorf("claim %s/%s not found", namespace, name)
	}
	return claim, nil
}

type fakeSlices map[string][]*resourceapi.ResourceSlice

func (f fakeSlices) forNode(nodeName string) []*resourceapi.ResourceSlice {
	return f[nodeName]
}

func testClaim(name string, uid types.UID, cpus int) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: uid},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{{
					Name: "cpus",
					Exactly: &resourceapi.ExactDeviceRequest{
						DeviceClassName: deviceClassName,
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
}

// testSlice publishes two NUMA devices in driver order with a min/step
// request policy, mirroring the fork's grouped layout.
func testSlice(nodeName string) *resourceapi.ResourceSlice {
	device := func(name string, numa int64) resourceapi.Device {
		return resourceapi.Device{
			Name:       name,
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{numaAttribute: {IntValue: &numa}},
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				capacityName: {
					Value: *resource.NewQuantity(126, resource.DecimalSI),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: ptr.To(resource.MustParse("126")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min:  ptr.To(resource.MustParse("2")),
							Step: ptr.To(resource.MustParse("2")),
						},
					},
				},
			},
		}
	}
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-slice"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driverName,
			NodeName: ptr.To(nodeName),
			Devices:  []resourceapi.Device{device("cpudevnuma000", 0), device("cpudevnuma001", 1)},
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

func annotatedNode(name string, report fitReport) *v1.Node {
	raw, _ := json.Marshal(report)
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        name,
		Annotations: map[string]string{FitAnnotation: string(raw)},
	}}
}

func nodeInfoOf(node *v1.Node) *framework.NodeInfo {
	info := framework.NewNodeInfo()
	info.SetNode(node)
	return info
}

func testPlugin(claims fakeClaims, slices fakeSlices) *CCXAlign {
	return &CCXAlign{claims: claims, slices: slices, reservations: newReservations()}
}

func preparedState(t *testing.T, plugin *CCXAlign, pod *v1.Pod) fwk.CycleState {
	t.Helper()
	state := framework.NewCycleState()
	if _, status := plugin.PreFilter(context.Background(), state, pod, nil); status != nil && !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	return state
}

func TestPreFilterSkipsPodsWithoutClaims(t *testing.T) {
	plugin := testPlugin(fakeClaims{}, fakeSlices{})
	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, testPod("p0"), nil)
	if status == nil || status.Code() != fwk.Skip {
		t.Fatalf("claimless pod: want Skip, got %v", status)
	}
	if got := plugin.PreScore(context.Background(), state, testPod("p0"), nil); got == nil || got.Code() != fwk.Skip {
		t.Fatalf("PreScore must skip scoring without state, got %v", got)
	}
}

func TestScoreReadsAnnotationRoundsAndReserves(t *testing.T) {
	claims := fakeClaims{"ns/claim-a": testClaim("claim-a", "uid-a", 15)} // rounds up to 16
	slices := fakeSlices{"worker": {testSlice("worker")}}
	plugin := testPlugin(claims, slices)
	pod := testPod("p1", "claim-a")
	state := preparedState(t, plugin, pod)

	report := fitReport{V: 1, Policy: policySpread, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16, 16}, FreeCPUs: []int{16, 2}, RepackedFreeCPUs: []int{16, 2}},
		{ID: 1, CacheCPUs: []int{16, 16}, FreeCPUs: []int{16, 2}, RepackedFreeCPUs: []int{16, 2}},
	}}
	node := annotatedNode("worker", report)

	score, status := plugin.Score(context.Background(), state, pod, nodeInfoOf(node))
	if !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	if score != scoreAlignedNow {
		t.Fatalf("15 CPUs round to a whole cache on numa0: want %d, got %d", scoreAlignedNow, score)
	}

	// Reserving the claim makes the next identical pod see numa0 consumed.
	if status := plugin.Reserve(context.Background(), state, pod, "worker"); !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	claims["ns/claim-b"] = testClaim("claim-b", "uid-b", 16)
	pod2 := testPod("p2", "claim-b")
	state2 := preparedState(t, plugin, pod2)
	score, status = plugin.Score(context.Background(), state2, pod2, nodeInfoOf(node))
	if !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	if score != scoreAlignedNow {
		t.Fatalf("numa1 still holds a whole cache: want %d, got %d", scoreAlignedNow, score)
	}
	// A third whole-cache claim finds both consumed: split at best.
	if status := plugin.Reserve(context.Background(), state2, pod2, "worker"); !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	claims["ns/claim-c"] = testClaim("claim-c", "uid-c", 16)
	pod3 := testPod("p3", "claim-c")
	state3 := preparedState(t, plugin, pod3)
	score, _ = plugin.Score(context.Background(), state3, pod3, nodeInfoOf(node))
	if score >= scoreAlignedRepacked {
		t.Fatalf("both caches reserved: want below the repack tier, got %d", score)
	}

	// Unreserve of pod2 releases exactly its claim.
	plugin.Unreserve(context.Background(), state2, pod2, "worker")
	score, _ = plugin.Score(context.Background(), state3, pod3, nodeInfoOf(node))
	if score != scoreAlignedNow {
		t.Fatalf("after unreserve a whole cache is back: want %d, got %d", scoreAlignedNow, score)
	}
	// Unreserve is idempotent and safe for pods that never reserved.
	plugin.Unreserve(context.Background(), state2, pod2, "worker")
	plugin.Unreserve(context.Background(), framework.NewCycleState(), testPod("stranger"), "worker")
}

func TestReservationsExpireAndFollowTheClaim(t *testing.T) {
	claims := fakeClaims{"ns/claim-a": testClaim("claim-a", "uid-a", 16)}
	slices := fakeSlices{"worker": {testSlice("worker")}}
	plugin := testPlugin(claims, slices)
	now := time.Now()
	plugin.reservations.now = func() time.Time { return now }

	pod := testPod("p1", "claim-a")
	state := preparedState(t, plugin, pod)
	if status := plugin.Reserve(context.Background(), state, pod, "worker"); !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	if got := plugin.reservations.needsFor("worker"); len(got) != 1 || got[0] != 16 {
		t.Fatalf("want one 16-CPU reservation, got %v", got)
	}
	// The claim informer clears it when the claim dies.
	plugin.reservations.removeClaim("uid-a")
	if got := plugin.reservations.needsFor("worker"); len(got) != 0 {
		t.Fatalf("claim death must clear the reservation, got %v", got)
	}
	// And the TTL is the backstop.
	if status := plugin.Reserve(context.Background(), state, pod, "worker"); !status.IsSuccess() {
		t.Fatal(status.AsError())
	}
	now = now.Add(reservationTTL + time.Second)
	if got := plugin.reservations.needsFor("worker"); len(got) != 0 {
		t.Fatalf("expired reservation must be pruned, got %v", got)
	}
}

func TestScoreDegradesToNeutralNeverErrors(t *testing.T) {
	claims := fakeClaims{"ns/claim-a": testClaim("claim-a", "uid-a", 16)}
	slices := fakeSlices{"worker": {testSlice("worker")}}
	plugin := testPlugin(claims, slices)
	pod := testPod("p1", "claim-a")
	state := preparedState(t, plugin, pod)

	for name, node := range map[string]*v1.Node{
		"no annotation": {ObjectMeta: metav1.ObjectMeta{Name: "worker"}},
		"garbage": {ObjectMeta: metav1.ObjectMeta{Name: "worker",
			Annotations: map[string]string{FitAnnotation: "{"}}},
		"newer version": {ObjectMeta: metav1.ObjectMeta{Name: "worker",
			Annotations: map[string]string{FitAnnotation: `{"v":9,"numaNodes":[{"id":0,"cacheCPUs":[16],"freeCPUs":[16],"repackedFreeCPUs":[16]}]}`}}},
		"empty numa": {ObjectMeta: metav1.ObjectMeta{Name: "worker",
			Annotations: map[string]string{FitAnnotation: `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[],"freeCPUs":[],"repackedFreeCPUs":[]}]}`}}},
	} {
		score, status := plugin.Score(context.Background(), state, pod, nodeInfoOf(node))
		if !status.IsSuccess() {
			t.Fatalf("%s: Score must never error, got %v", name, status.AsError())
		}
		if score != scoreSplit {
			t.Fatalf("%s: unknown shape must score neutral %d, got %d", name, scoreSplit, score)
		}
	}

	// A node without dra.cpu slices scores neutral too.
	bare := testPlugin(claims, fakeSlices{})
	score, status := bare.Score(context.Background(), state, pod, nodeInfoOf(annotatedNode("worker", fitReport{V: 1, NUMA: []numaFit{{ID: 0, CacheCPUs: []int{16}, FreeCPUs: []int{16}, RepackedFreeCPUs: []int{16}}}})))
	if !status.IsSuccess() || score != scoreSplit {
		t.Fatalf("no slices: want neutral, got %d (%v)", score, status)
	}
}

func TestPodNeedsResolvesDirectAndAllocatedClaims(t *testing.T) {
	allocated := testClaim("claim-done", "uid-done", 16)
	allocated.Status.Allocation = &resourceapi.AllocationResult{}
	claims := fakeClaims{
		"ns/claim-a":    testClaim("claim-a", "uid-a", 6),
		"ns/claim-done": allocated,
	}
	plugin := testPlugin(claims, fakeSlices{})

	pod := testPod("p1", "claim-a", "claim-done")
	needs := plugin.podNeeds(pod)
	if len(needs) != 1 || needs[0].cpus != 6 || needs[0].claimUID != "uid-a" {
		t.Fatalf("want only the unallocated claim's need, got %+v", needs)
	}

	// Template-generated claims resolve through the pod status.
	templated := testPod("p2")
	templated.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "tmpl", ResourceClaimTemplateName: ptr.To("tmpl")}}
	templated.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{{Name: "tmpl", ResourceClaimName: ptr.To("claim-a")}}
	needs = plugin.podNeeds(templated)
	if len(needs) != 1 || needs[0].claimUID != "uid-a" {
		t.Fatalf("template resolution failed, got %+v", needs)
	}
}

// TestReservationsKeepEveryDeviceOfAClaim: a claim asking for several devices
// gets several reservations, since each lands on its own device. Keyed by
// claim UID alone they would overwrite each other and the claim would reserve
// the space of one device however many it actually asked for.
func TestReservationsKeepEveryDeviceOfAClaim(t *testing.T) {
	r := newReservations()
	r.add("node-a", "pod-1", "claim-1", 0, 16)
	r.add("node-a", "pod-1", "claim-1", 1, 16)

	needs := r.needsFor("node-a")
	if len(needs) != 2 {
		t.Fatalf("both devices of the claim must be reserved, got %v", needs)
	}

	r.removeClaim("claim-1")
	if left := r.needsFor("node-a"); len(left) != 0 {
		t.Fatalf("releasing the claim must release every device, got %v", left)
	}
}

// TestDeviceOrderPolicyMinWithoutStep: ValidRange requires Min while Step is
// optional, so a driver may legally publish a minimum alone. Parsed only under
// a Step, the minimum would be lost and the plugin would score a demand the
// allocator will never debit.
func TestDeviceOrderPolicyMinWithoutStep(t *testing.T) {
	slice := testSlice("node-a")
	for i := range slice.Spec.Devices {
		slice.Spec.Devices[i].Capacity[capacityName] = resourceapi.DeviceCapacity{
			Value: *resource.NewQuantity(126, resource.DecimalSI),
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: ptr.To(resource.MustParse("126")),
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min: ptr.To(resource.MustParse("4")),
				},
			},
		}
	}
	plugin := &CCXAlign{slices: fakeSlices{"node-a": {slice}}}

	order, policy := plugin.deviceOrder("node-a")
	if len(order) != 2 {
		t.Fatalf("device order lost: %v", order)
	}
	if policy.minCPUs != 4 || policy.step != 0 {
		t.Fatalf("policy = %+v, want minCPUs=4 step=0", policy)
	}
	if got := policy.round(1); got != 4 {
		t.Fatalf("round(1) = %d, want the driver's minimum 4", got)
	}
}
