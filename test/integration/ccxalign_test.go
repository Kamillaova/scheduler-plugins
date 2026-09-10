package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler"
	schedapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/scheduler-plugins/pkg/ccxalign"
)

const (
	ccxDriverName        = "dra.cpu"
	ccxCapacityName      = "dra.cpu/cpu"
	ccxNumaAttr          = "dra.cpu/numaNodeID"
	ccxCacheL3IDAttr     = "dra.cpu/cacheL3ID"
	ccxPartitionAttr     = "dra.cpu/partition"
	ccxRepairRoundsAttr  = "dra.cpu/repairRounds"
	ccxFrontierInputAttr = "dra.cpu/frontierInput"

	ccxSchedulerName       = "dracpu-scheduler"
	ccxLeastAllocatedSched = "dracpu-leastallocated-scheduler"
)

func makeCCXNode(name string, cpus int64, mem string) *v1.Node {
	return st.MakeNode().Name(name).Capacity(map[v1.ResourceName]string{
		v1.ResourceCPU:    fmt.Sprintf("%d", cpus),
		v1.ResourceMemory: mem,
		v1.ResourcePods:   "32",
	}).Obj()
}

func makeCCXDeviceClass(name string) *resourceapi.DeviceClass {
	return &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourceapi.DeviceClassSpec{
			Selectors: []resourceapi.DeviceSelector{{
				CEL: &resourceapi.CELDeviceSelector{
					Expression: fmt.Sprintf("device.driver == %q", ccxDriverName),
				},
			}},
		},
	}
}

func makeCCXCacheDevice(name string, numaID int64, cacheID int64, capacity int, partition, repairRounds, frontierInput string) resourceapi.Device {
	dev := resourceapi.Device{
		Name:                     name,
		AllowMultipleAllocations: ptr.To(true),
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			ccxNumaAttr:      {IntValue: &numaID},
			ccxCacheL3IDAttr: {IntValue: &cacheID},
		},
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			ccxCapacityName: {
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
		dev.Attributes[ccxPartitionAttr] = resourceapi.DeviceAttribute{StringValue: &partition}
	}
	if repairRounds != "" {
		dev.Attributes[ccxRepairRoundsAttr] = resourceapi.DeviceAttribute{StringValue: &repairRounds}
	}
	if frontierInput != "" {
		dev.Attributes[ccxFrontierInputAttr] = resourceapi.DeviceAttribute{StringValue: &frontierInput}
	}
	return dev
}

func makeCCXResourceSlice(sliceName, nodeName string, devices ...resourceapi.Device) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: sliceName},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   ccxDriverName,
			NodeName: ptr.To(nodeName),
			Pool:     resourceapi.ResourcePool{Name: nodeName, Generation: 1, ResourceSliceCount: 1},
			Devices:  devices,
		},
	}
}

func makeCCXClaim(namespace, name string, count int, cpus int64, alignment v1alpha1.Alignment, relocatable bool, partition string, splitAlternatives bool) *resourceapi.ResourceClaim {
	cfg := v1alpha1.OpaqueConfig{
		APIVersion: v1alpha1.APIVersion,
		CPUConfig: v1alpha1.CPUConfig{
			Relocatable: relocatable,
			Alignment:   alignment,
		},
	}
	raw, _ := json.Marshal(cfg)

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Config: []resourceapi.DeviceClaimConfiguration{
					{
						DeviceConfiguration: resourceapi.DeviceConfiguration{
							Opaque: &resourceapi.OpaqueDeviceConfiguration{
								Driver:     ccxDriverName,
								Parameters: runtime.RawExtension{Raw: raw},
							},
						},
					},
				},
			},
		},
	}

	if splitAlternatives {
		splitCount := count * 2
		splitCPUs := cpus / 2
		if splitCPUs < 2 {
			splitCPUs = 2
		}
		claim.Spec.Devices.Requests = []resourceapi.DeviceRequest{
			{
				Name: "req-cpu",
				FirstAvailable: []resourceapi.DeviceSubRequest{
					{
						Name:            "aligned",
						DeviceClassName: ccxDriverName,
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           int64(count),
						Capacity: &resourceapi.CapacityRequirements{
							Requests: map[resourceapi.QualifiedName]resource.Quantity{
								ccxCapacityName: *resource.NewQuantity(cpus, resource.DecimalSI),
							},
						},
					},
					{
						Name:            "split2",
						DeviceClassName: ccxDriverName,
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           int64(splitCount),
						Capacity: &resourceapi.CapacityRequirements{
							Requests: map[resourceapi.QualifiedName]resource.Quantity{
								ccxCapacityName: *resource.NewQuantity(splitCPUs, resource.DecimalSI),
							},
						},
					},
				},
			},
		}
	} else {
		claim.Spec.Devices.Requests = []resourceapi.DeviceRequest{
			{
				Name: "req-cpu",
				Exactly: &resourceapi.ExactDeviceRequest{
					DeviceClassName: ccxDriverName,
					AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
					Count:           int64(count),
					Capacity: &resourceapi.CapacityRequirements{
						Requests: map[resourceapi.QualifiedName]resource.Quantity{
							ccxCapacityName: *resource.NewQuantity(cpus, resource.DecimalSI),
						},
					},
				},
			},
		}
	}

	if partition != "" && partition != "default" {
		if splitAlternatives {
			for i := range claim.Spec.Devices.Requests[0].FirstAvailable {
				claim.Spec.Devices.Requests[0].FirstAvailable[i].Tolerations = []resourceapi.DeviceToleration{
					{Key: ccxPartitionAttr, Value: partition},
				}
			}
		} else {
			claim.Spec.Devices.Requests[0].Exactly.Tolerations = []resourceapi.DeviceToleration{
				{Key: ccxPartitionAttr, Value: partition},
			}
		}
	}

	return claim
}

func makeAllocatedCCXClaim(namespace, name, nodeName string, devices []string, cpusPerDevice int64) *resourceapi.ResourceClaim {
	claim := makeCCXClaim(namespace, name, len(devices), cpusPerDevice, v1alpha1.AlignmentBestEffort, false, "default", false)
	var results []resourceapi.DeviceRequestAllocationResult
	for _, dev := range devices {
		shareUID := types.UID(uuid.NewUUID())
		results = append(results, resourceapi.DeviceRequestAllocationResult{
			Request: "req-cpu",
			Driver:  ccxDriverName,
			Pool:    nodeName,
			Device:  dev,
			ShareID: &shareUID,
			ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{
				ccxCapacityName: *resource.NewQuantity(cpusPerDevice, resource.DecimalSI),
			},
		})
	}
	claim.Status = resourceapi.ResourceClaimStatus{
		Allocation: &resourceapi.AllocationResult{
			NodeSelector: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{nodeName},
							},
						},
					},
				},
			},
			Devices: resourceapi.DeviceAllocationResult{
				Results: results,
			},
		},
	}
	return claim
}

func makeCCXPod(namespace, name, schedulerName string, claimNames ...string) *v1.Pod {
	pod := st.MakePod().Namespace(namespace).Name(name).SchedulerName(schedulerName).Obj()
	pod.Spec.Containers = []v1.Container{
		{
			Name:  "app",
			Image: "registry.k8s.io/pause:3.9",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		},
	}
	for _, cName := range claimNames {
		pod.Spec.ResourceClaims = append(pod.Spec.ResourceClaims, v1.PodResourceClaim{
			Name:              cName,
			ResourceClaimName: ptr.To(cName),
		})
		pod.Spec.Containers[0].Resources.Claims = append(pod.Spec.Containers[0].Resources.Claims, v1.ResourceClaim{
			Name: cName,
		})
	}
	return pod
}

func makeCCXProfile(schedulerName string, mode ccxalign.ScoringStrategyType) schedapi.KubeSchedulerProfile {
	return schedapi.KubeSchedulerProfile{
		SchedulerName:            schedulerName,
		PercentageOfNodesToScore: ptr.To(int32(100)),
		Plugins: &schedapi.Plugins{
			QueueSort: schedapi.PluginSet{
				Enabled: []schedapi.Plugin{
					{Name: queuesort.Name},
				},
			},
			MultiPoint: schedapi.PluginSet{
				Enabled: []schedapi.Plugin{
					{Name: ccxalign.Name},
				},
			},
			Score: schedapi.PluginSet{
				Enabled: []schedapi.Plugin{
					{Name: ccxalign.Name, Weight: 10},
				},
				Disabled: []schedapi.Plugin{
					{Name: "ImageLocality"},
					{Name: "PodTopologySpread"},
					{Name: "DynamicResources"},
				},
			},
			Bind: schedapi.PluginSet{
				Enabled: []schedapi.Plugin{
					{Name: defaultbinder.Name},
				},
			},
		},
		PluginConfig: []schedapi.PluginConfig{
			{
				Name: ccxalign.Name,
				Args: &ccxalign.CCXAlignArgs{
					ScoringStrategy: &ccxalign.ScoringStrategy{
						Type: mode,
					},
				},
			},
		},
	}
}

func setupCCXTestCluster(t *testing.T) (*testContext, clientset.Interface) {
	testCtx := &testContext{}
	testCtx.Ctx, testCtx.CancelFn = context.WithCancel(context.Background())
	testCtx.ClientSet = clientset.NewForConfigOrDie(globalKubeConfig)
	testCtx.KubeConfig = globalKubeConfig

	profiles := []schedapi.KubeSchedulerProfile{
		makeCCXProfile(ccxSchedulerName, ccxalign.MostAllocated),
		makeCCXProfile(ccxLeastAllocatedSched, ccxalign.LeastAllocated),
	}

	testCtx = initTestSchedulerWithOptions(
		t,
		testCtx,
		scheduler.WithProfiles(profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{ccxalign.Name: ccxalign.New}),
	)
	syncInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)

	class := makeCCXDeviceClass(ccxDriverName)
	_, err := testCtx.ClientSet.ResourceV1().DeviceClasses().Create(testCtx.Ctx, class, metav1.CreateOptions{})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Failed to create DeviceClass: %v", err)
	}

	return testCtx, testCtx.ClientSet
}

func cleanupCCXObjects(t *testing.T, cs clientset.Interface, ns string, nodes []*v1.Node, slices []*resourceapi.ResourceSlice, claims []*resourceapi.ResourceClaim) {
	ctx := context.Background()
	for _, c := range claims {
		_ = cs.ResourceV1().ResourceClaims(ns).Delete(ctx, c.Name, metav1.DeleteOptions{})
	}
	for _, s := range slices {
		_ = cs.ResourceV1().ResourceSlices().Delete(ctx, s.Name, metav1.DeleteOptions{})
	}
	for _, n := range nodes {
		_ = cs.CoreV1().Nodes().Delete(ctx, n.Name, metav1.DeleteOptions{})
	}
}

func make8CCXCaches(repairRounds, digest string) []resourceapi.Device {
	devs := make([]resourceapi.Device, 8)
	for i := 0; i < 8; i++ {
		devs[i] = makeCCXCacheDevice(fmt.Sprintf("cache-%d", i), 0, int64(i), 16, "default", repairRounds, digest)
	}
	return devs
}

var all8CCXDevNames = []string{
	"cache-0", "cache-1", "cache-2", "cache-3",
	"cache-4", "cache-5", "cache-6", "cache-7",
}

func TestCCXAlign_CodebookGoldenVectors(t *testing.T) {
	type goldenVectorCase struct {
		name          string
		mode          ccxalign.ScoringStrategyType
		devices       []resourceapi.Device
		claim         *resourceapi.ResourceClaim
		occupiedDevs  []string
		minBand       int64
		maxBand       int64
		expectedScore int64
		expectedTier  string
	}

	cases := []goldenVectorCase{
		{
			name: "WarmAlignedPacking: occupied cache gives 80 base plus 8 reuse bonus",
			mode: ccxalign.MostAllocated,
			devices: []resourceapi.Device{
				makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
			},
			occupiedDevs:  []string{"cache-0"},
			claim:         makeCCXClaim("default", "c1", 1, 8, v1alpha1.AlignmentBestEffort, false, "default", false),
			minBand:       80,
			maxBand:       88,
			expectedScore: 88,
			expectedTier:  ccxalign.TierLabelWarmAligned,
		},
		{
			name:          "Repairable1RoundPacking: 1 round repair gives 60 base plus 8 bonus",
			mode:          ccxalign.MostAllocated,
			devices:       make8CCXCaches("1,1,1,1", api.FrontierInputDigest([]string{"fill-0", "fill-1"})),
			occupiedDevs:  all8CCXDevNames,
			claim:         makeCCXClaim("default", "c2", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true),
			minBand:       60,
			maxBand:       68,
			expectedScore: 68,
			expectedTier:  ccxalign.TierLabelRepairable,
		},
		{
			name: "ColdAlignedPacking: completely clean node gives 40 base plus 0 bonus",
			mode: ccxalign.MostAllocated,
			devices: []resourceapi.Device{
				makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
			},
			occupiedDevs:  nil,
			claim:         makeCCXClaim("default", "c3", 1, 8, v1alpha1.AlignmentBestEffort, false, "default", false),
			minBand:       40,
			maxBand:       48,
			expectedScore: 40,
			expectedTier:  ccxalign.TierLabelColdAligned,
		},
		{
			name:          "Repairable2RoundPacking: 2 round repair gives 40 base plus 8 bonus, sharing 40..48 band with cold-aligned",
			mode:          ccxalign.MostAllocated,
			devices:       make8CCXCaches("2,2,2,2", api.FrontierInputDigest([]string{"fill-0", "fill-1"})),
			occupiedDevs:  all8CCXDevNames,
			claim:         makeCCXClaim("default", "c4", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true),
			minBand:       40,
			maxBand:       48,
			expectedScore: 48,
			expectedTier:  ccxalign.TierLabelRepairable,
		},
		{
			name:          "Repairable3RoundPacking: 3 round repair gives 20 base plus 8 bonus",
			mode:          ccxalign.MostAllocated,
			devices:       make8CCXCaches("3,3,3,3", api.FrontierInputDigest([]string{"fill-0", "fill-1"})),
			occupiedDevs:  all8CCXDevNames,
			claim:         makeCCXClaim("default", "c5", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true),
			minBand:       20,
			maxBand:       28,
			expectedScore: 28,
			expectedTier:  ccxalign.TierLabelRepairable,
		},
		{
			name:          "SplitPacking: unreachable repair gives 0 base plus 8 bonus",
			mode:          ccxalign.MostAllocated,
			devices:       make8CCXCaches("-,-,-,-", api.FrontierInputDigest([]string{"fill-0", "fill-1"})),
			occupiedDevs:  all8CCXDevNames,
			claim:         makeCCXClaim("default", "c6", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true),
			minBand:       0,
			maxBand:       8,
			expectedScore: 8,
			expectedTier:  ccxalign.TierLabelSplit,
		},
		{
			name: "SubCacheCleanPacking: sub-cache claim on clean cache receives 0 bonus",
			mode: ccxalign.MostAllocated,
			devices: []resourceapi.Device{
				makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
			},
			occupiedDevs:  nil,
			claim:         makeCCXClaim("default", "c7", 1, 8, v1alpha1.AlignmentBestEffort, false, "default", false),
			minBand:       40,
			maxBand:       48,
			expectedScore: 40,
			expectedTier:  ccxalign.TierLabelColdAligned,
		},
		{
			name: "ColdAlignedSpreading: spreading mode awards 80 base to aligned on cold node",
			mode: ccxalign.LeastAllocated,
			devices: []resourceapi.Device{
				makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
				makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "", ""),
			},
			occupiedDevs:  nil,
			claim:         makeCCXClaim("default", "c8", 1, 8, v1alpha1.AlignmentBestEffort, false, "default", false),
			minBand:       80,
			maxBand:       88,
			expectedScore: 82,
			expectedTier:  ccxalign.TierLabelWarmAligned,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			score, tier, _ := ccxalign.ScoreTestClaim(
				[]*resourceapi.ResourceClaim{tc.claim},
				tc.devices,
				tc.occupiedDevs,
				tc.mode,
			)
			if score < tc.minBand || score > tc.maxBand {
				t.Errorf("Score %d outside expected band [%d..%d]", score, tc.minBand, tc.maxBand)
			}
			if score != tc.expectedScore {
				t.Errorf("Score mismatch: got %d, want %d", score, tc.expectedScore)
			}
			if tier != tc.expectedTier {
				t.Errorf("Tier mismatch: got %q, want %q", tier, tc.expectedTier)
			}
		})
	}
}

func TestCCXAlign_FilterUniversalRule(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-univ-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	nodeAsym := makeCCXNode("asym-node", 64, "128Gi")
	nodeSym := makeCCXNode("sym-node", 64, "128Gi")

	for _, n := range []*v1.Node{nodeAsym, nodeSym} {
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node %s: %v", n.Name, err)
		}
	}

	fillAsym0 := makeAllocatedCCXClaim(ns, "fill-asym-0", "asym-node", []string{"cache-0", "cache-1"}, 8)
	createdAsym0, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fillAsym0, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fillAsym0: %v", err)
	}
	createdAsym0.Status = fillAsym0.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdAsym0, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for fillAsym0: %v", err)
	}

	fillAsym1 := makeAllocatedCCXClaim(ns, "fill-asym-1", "asym-node", []string{"cache-2", "cache-3"}, 8)
	createdAsym1, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fillAsym1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fillAsym1: %v", err)
	}
	createdAsym1.Status = fillAsym1.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdAsym1, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for fillAsym1: %v", err)
	}

	fillSym0 := makeAllocatedCCXClaim(ns, "fill-sym-0", "sym-node", []string{"cache-0", "cache-1"}, 8)
	createdSym0, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fillSym0, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fillSym0: %v", err)
	}
	createdSym0.Status = fillSym0.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdSym0, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for fillSym0: %v", err)
	}

	fillSym1 := makeAllocatedCCXClaim(ns, "fill-sym-1", "sym-node", []string{"cache-2", "cache-3"}, 8)
	createdSym1, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fillSym1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fillSym1: %v", err)
	}
	createdSym1.Status = fillSym1.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdSym1, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for fillSym1: %v", err)
	}

	digestAsym0 := api.FrontierInputDigest([]string{string(createdAsym0.UID)})
	digestAsym1 := api.FrontierInputDigest([]string{string(createdAsym1.UID)})
	digestSym0 := api.FrontierInputDigest([]string{string(createdSym0.UID)})
	digestSym1 := api.FrontierInputDigest([]string{string(createdSym1.UID)})

	sliceAsym := makeCCXResourceSlice("asym-slice", "asym-node",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "1,1,1,1", digestAsym0),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "1,1,1,1", digestAsym0),
		makeCCXCacheDevice("cache-2", 1, 2, 16, "default", "-,-,-,-", digestAsym1),
		makeCCXCacheDevice("cache-3", 1, 3, 16, "default", "-,-,-,-", digestAsym1),
	)

	sliceSym := makeCCXResourceSlice("sym-slice", "sym-node",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "1,1,1,1", digestSym0),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "1,1,1,1", digestSym0),
		makeCCXCacheDevice("cache-2", 1, 2, 16, "default", "1,1,1,1", digestSym1),
		makeCCXCacheDevice("cache-3", 1, 3, 16, "default", "1,1,1,1", digestSym1),
	)

	for _, s := range []*resourceapi.ResourceSlice{sliceAsym, sliceSym} {
		if _, err := cs.ResourceV1().ResourceSlices().Create(testCtx.Ctx, s, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create slice %s: %v", s.Name, err)
		}
	}

	claim := makeCCXClaim(ns, "repair-claim", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true)
	if _, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, claim, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create test claim: %v", err)
	}

	pod := makeCCXPod(ns, "repair-pod", ccxSchedulerName, "repair-claim")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create test pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, pod.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for repair-pod to schedule: %v", err)
	}

	nodeName, err := getNodeName(testCtx.Ctx, cs, ns, pod.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeName for scheduled pod: %v", err)
	}
	if nodeName != "sym-node" {
		t.Errorf("Universal rule failed: pod scheduled on %q, expected %q", nodeName, "sym-node")
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{nodeAsym, nodeSym}, []*resourceapi.ResourceSlice{sliceAsym, sliceSym}, []*resourceapi.ResourceClaim{createdAsym0, createdAsym1, createdSym0, createdSym1, claim})
}

func TestCCXAlign_FrontierDigestFreshness(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-fresh-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	node := makeCCXNode("fresh-node", 64, "128Gi")
	if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	fill := makeAllocatedCCXClaim(ns, "fresh-fill", "fresh-node", []string{"cache-0", "cache-1"}, 8)
	createdFill, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fill, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fill claim: %v", err)
	}
	createdFill.Status = fill.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdFill, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	slice := makeCCXResourceSlice("fresh-slice", "fresh-node",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "1,1,1,1", "sha256:stale-digest"),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "1,1,1,1", "sha256:stale-digest"),
	)
	if _, err := cs.ResourceV1().ResourceSlices().Create(testCtx.Ctx, slice, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create slice: %v", err)
	}

	claim := makeCCXClaim(ns, "fresh-claim", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true)
	if _, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, claim, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create claim: %v", err)
	}

	pod := makeCCXPod(ns, "fresh-pod", ccxSchedulerName, "fresh-claim")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	time.Sleep(1 * time.Second)
	if podScheduled(t, cs, ns, pod.Name) {
		t.Fatal("Pod scheduled despite stale frontier input digest")
	}

	expectedDigest := api.FrontierInputDigest([]string{string(createdFill.UID)})
	latestSlice, err := cs.ResourceV1().ResourceSlices().Get(testCtx.Ctx, slice.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get slice: %v", err)
	}
	for i := range latestSlice.Spec.Devices {
		latestSlice.Spec.Devices[i].Attributes[ccxFrontierInputAttr] = resourceapi.DeviceAttribute{StringValue: &expectedDigest}
	}
	if _, err := cs.ResourceV1().ResourceSlices().Update(testCtx.Ctx, latestSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update slice with fresh digest: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, pod.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for fresh-pod to schedule after digest refresh: %v", err)
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{node}, []*resourceapi.ResourceSlice{latestSlice}, []*resourceapi.ResourceClaim{createdFill, claim})
}

func TestCCXAlign_RequeueOnSliceUpdate(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-requeue-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	node := makeCCXNode("requeue-node", 64, "128Gi")
	if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	fill := makeAllocatedCCXClaim(ns, "fill-requeue", "requeue-node", []string{"cache-0", "cache-1"}, 8)
	createdFill, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fill, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fill claim: %v", err)
	}
	createdFill.Status = fill.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdFill, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	digest := api.FrontierInputDigest([]string{string(createdFill.UID)})
	slice := makeCCXResourceSlice("requeue-slice", "requeue-node",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "-,-,-,-", digest),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "-,-,-,-", digest),
	)
	if _, err := cs.ResourceV1().ResourceSlices().Create(testCtx.Ctx, slice, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create slice: %v", err)
	}

	claim := makeCCXClaim(ns, "requeue-claim", 1, 16, v1alpha1.AlignmentRepairable, true, "default", true)
	if _, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, claim, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create claim: %v", err)
	}

	pod := makeCCXPod(ns, "requeue-pod", ccxSchedulerName, "requeue-claim")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	time.Sleep(1 * time.Second)
	if podScheduled(t, cs, ns, pod.Name) {
		t.Fatal("Pod scheduled despite unreachable frontier")
	}

	latestSlice, err := cs.ResourceV1().ResourceSlices().Get(testCtx.Ctx, slice.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get slice: %v", err)
	}
	reachableRounds := "1,1,1,1"
	for i := range latestSlice.Spec.Devices {
		latestSlice.Spec.Devices[i].Attributes[ccxRepairRoundsAttr] = resourceapi.DeviceAttribute{StringValue: &reachableRounds}
	}
	if _, err := cs.ResourceV1().ResourceSlices().Update(testCtx.Ctx, latestSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update slice: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, pod.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for requeued pod to schedule: %v", err)
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{node}, []*resourceapi.ResourceSlice{latestSlice}, []*resourceapi.ResourceClaim{createdFill, claim})
}

func TestCCXAlign_MultiClaimJointOverlay(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-multi-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	nodeA := makeCCXNode("node-a", 64, "128Gi")
	nodeB := makeCCXNode("node-b", 64, "128Gi")

	for _, n := range []*v1.Node{nodeA, nodeB} {
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node %s: %v", n.Name, err)
		}
	}

	sliceA := makeCCXResourceSlice("slice-a", "node-a",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "", ""),
	)
	sliceB := makeCCXResourceSlice("slice-b", "node-b",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
		makeCCXCacheDevice("cache-1", 0, 1, 16, "default", "", ""),
	)

	for _, s := range []*resourceapi.ResourceSlice{sliceA, sliceB} {
		if _, err := cs.ResourceV1().ResourceSlices().Create(testCtx.Ctx, s, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create slice %s: %v", s.Name, err)
		}
	}

	fillB := makeAllocatedCCXClaim(ns, "fill-b", "node-b", []string{"cache-1"}, 12)
	createdFillB, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, fillB, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create fillB: %v", err)
	}
	createdFillB.Status = fillB.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdFillB, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for fillB: %v", err)
	}

	claim1 := makeCCXClaim(ns, "joint-c1", 1, 16, v1alpha1.AlignmentBestEffort, false, "default", false)
	claim2 := makeCCXClaim(ns, "joint-c2", 1, 16, v1alpha1.AlignmentBestEffort, false, "default", false)

	for _, c := range []*resourceapi.ResourceClaim{claim1, claim2} {
		if _, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, c, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create claim %s: %v", c.Name, err)
		}
	}

	pod := makeCCXPod(ns, "joint-pod", ccxSchedulerName, "joint-c1", "joint-c2")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, pod.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for joint-pod to schedule: %v", err)
	}

	nodeName, err := getNodeName(testCtx.Ctx, cs, ns, pod.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeName: %v", err)
	}
	if nodeName != "node-a" {
		t.Errorf("Expected joint-pod to schedule on %q with higher joint tier, got %q", "node-a", nodeName)
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{nodeA, nodeB}, []*resourceapi.ResourceSlice{sliceA, sliceB}, []*resourceapi.ResourceClaim{createdFillB, claim1, claim2})
}

func TestCCXAlign_ReservedForAndAllocatedPinned(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-res-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	node1 := makeCCXNode("pin-node-1", 64, "128Gi")
	node2 := makeCCXNode("pin-node-2", 64, "128Gi")

	for _, n := range []*v1.Node{node1, node2} {
		if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, n, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create node %s: %v", n.Name, err)
		}
	}

	slice1 := makeCCXResourceSlice("pin-slice-1", "pin-node-1",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
	)
	slice2 := makeCCXResourceSlice("pin-slice-2", "pin-node-2",
		makeCCXCacheDevice("cache-0", 0, 0, 16, "default", "", ""),
	)

	for _, s := range []*resourceapi.ResourceSlice{slice1, slice2} {
		if _, err := cs.ResourceV1().ResourceSlices().Create(testCtx.Ctx, s, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create slice %s: %v", s.Name, err)
		}
	}

	foreignClaim := makeAllocatedCCXClaim(ns, "foreign-claim", "pin-node-1", []string{"cache-0"}, 16)
	foreignClaim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{
			Resource: "pods",
			Name:     "other-pod",
			UID:      types.UID("other-pod-uid"),
		},
	}
	createdForeign, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, foreignClaim, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create foreign claim: %v", err)
	}
	createdForeign.Status = foreignClaim.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdForeign, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for foreign claim: %v", err)
	}

	podForeign := makeCCXPod(ns, "foreign-pod", ccxSchedulerName, "foreign-claim")
	podForeign.UID = types.UID("real-pod-uid")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, podForeign, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create foreign pod: %v", err)
	}

	time.Sleep(1 * time.Second)
	if podScheduled(t, cs, ns, podForeign.Name) {
		t.Fatal("Pod scheduled despite foreign ReservedFor entry")
	}

	pinnedClaim := makeAllocatedCCXClaim(ns, "pinned-claim", "pin-node-1", []string{"cache-0"}, 16)
	createdPinned, err := cs.ResourceV1().ResourceClaims(ns).Create(testCtx.Ctx, pinnedClaim, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pinned claim: %v", err)
	}
	createdPinned.Status = pinnedClaim.Status
	if _, err := cs.ResourceV1().ResourceClaims(ns).UpdateStatus(testCtx.Ctx, createdPinned, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Failed to update status for pinned claim: %v", err)
	}

	podPinned := makeCCXPod(ns, "pinned-pod", ccxSchedulerName, "pinned-claim")
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, podPinned, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pinned pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, podPinned.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for pinned-pod to schedule: %v", err)
	}

	nodeName, err := getNodeName(testCtx.Ctx, cs, ns, podPinned.Name)
	if err != nil {
		t.Fatalf("Failed to get nodeName: %v", err)
	}
	if nodeName != "pin-node-1" {
		t.Errorf("Expected pinned-pod to schedule on allocated node %q, got %q", "pin-node-1", nodeName)
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{node1, node2}, []*resourceapi.ResourceSlice{slice1, slice2}, []*resourceapi.ResourceClaim{createdForeign, createdPinned})
}

func TestCCXAlign_NonDriverPodUntouched(t *testing.T) {
	testCtx, cs := setupCCXTestCluster(t)
	defer cleanupTest(t, testCtx)

	ns := fmt.Sprintf("ccx-plain-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	node := makeCCXNode("plain-node", 64, "128Gi")
	if _, err := cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	pod := makeCCXPod(ns, "plain-pod", ccxSchedulerName)
	if _, err := cs.CoreV1().Pods(ns).Create(testCtx.Ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create plain pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(testCtx.Ctx, 100*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
		return podScheduled(t, cs, ns, pod.Name), nil
	}); err != nil {
		t.Fatalf("Timed out waiting for plain pod without claims to schedule: %v", err)
	}

	cleanupCCXObjects(t, cs, ns, []*v1.Node{node}, nil, nil)
}
