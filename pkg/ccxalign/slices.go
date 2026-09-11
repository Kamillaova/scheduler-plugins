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
	"fmt"
	"sort"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/client-go/tools/cache"
)

type nodePolicy struct {
	minCPUs int
	step    int
}

func (p nodePolicy) round(cpus int) int {
	if cpus < p.minCPUs {
		cpus = p.minCPUs
	}
	if p.step > 1 {
		cpus = (cpus + p.step - 1) / p.step * p.step
	}
	return cpus
}

type deviceInfo struct {
	name          string
	poolName      string
	numaNodeID    int
	cacheID       string
	partition     string
	totalCPUs     int
	largestCache  int
	repairRounds  string
	frontierInput string
	hasCacheID    bool
}

func devicePartition(dev resourceapi.Device) string {
	if attr, ok := dev.Attributes[partitionAttr]; ok && attr.StringValue != nil && *attr.StringValue != "" {
		return *attr.StringValue
	}
	return defaultPartition
}

func deviceNUMANode(dev resourceapi.Device) int {
	if attr, ok := dev.Attributes[standardNUMAAttr]; ok && attr.IntValue != nil {
		return int(*attr.IntValue)
	}
	if attr, ok := dev.Attributes[numaAttribute]; ok && attr.IntValue != nil {
		return int(*attr.IntValue)
	}
	return 0
}

func deviceRepairRounds(dev resourceapi.Device) string {
	if attr, ok := dev.Attributes[repairRoundsAttr]; ok && attr.StringValue != nil {
		return *attr.StringValue
	}
	if attr, ok := dev.Attributes[repairMovesAttr]; ok && attr.StringValue != nil {
		return *attr.StringValue
	}
	return ""
}

func deviceFrontierInput(dev resourceapi.Device) string {
	if attr, ok := dev.Attributes[frontierInputAttr]; ok && attr.StringValue != nil {
		return *attr.StringValue
	}
	return ""
}

// deviceLargestCache reports the CPUs of the node's largest uncore cache as the
// driver measured it from topology. The published capacity cannot answer this:
// the driver rewrites it while a claim sits on a cache it was not charged to,
// so it moves with defragmentation while the cache does not.
func deviceLargestCache(dev resourceapi.Device) int {
	if attr, ok := dev.Attributes[largestCacheAttr]; ok && attr.IntValue != nil {
		return int(*attr.IntValue)
	}
	return 0
}

func (p *CCXAlign) nodeDevices(nodeName string) ([]deviceInfo, nodePolicy) {
	var policy nodePolicy
	adopted := false
	var devices []deviceInfo

	nodeSlices := p.slices.forNode(nodeName)
	// The informer index hands slices back in map order, which would make the
	// landing prediction and the cache comparison depend on which call this
	// is. The experimental allocator, the one a capacity-carrying claim goes
	// through, sorts a pool's slices by name, so that order is both stable and
	// the one the prediction is measured against.
	sort.Slice(nodeSlices, func(i, j int) bool {
		if nodeSlices[i].Spec.Pool.Name != nodeSlices[j].Spec.Pool.Name {
			return nodeSlices[i].Spec.Pool.Name < nodeSlices[j].Spec.Pool.Name
		}
		return nodeSlices[i].Name < nodeSlices[j].Name
	})

	for _, slice := range nodeSlices {
		if slice.Spec.Driver != driverName {
			continue
		}
		poolName := slice.Spec.Pool.Name
		for _, dev := range slice.Spec.Devices {
			numaID := deviceNUMANode(dev)
			part := devicePartition(dev)
			repair := deviceRepairRounds(dev)
			input := deviceFrontierInput(dev)

			totalCPUs := 0
			if cap, ok := dev.Capacity[capacityName]; ok {
				totalCPUs = int(cap.Value.Value())
			}

			cacheID := dev.Name
			hasCacheID := false
			if cAttr, ok := dev.Attributes[cacheL3IDAttr]; ok && cAttr.IntValue != nil {
				cacheID = fmt.Sprintf("l3-%d", *cAttr.IntValue)
				hasCacheID = true
			}

			devices = append(devices, deviceInfo{
				name:          dev.Name,
				poolName:      poolName,
				numaNodeID:    numaID,
				cacheID:       cacheID,
				partition:     part,
				totalCPUs:     totalCPUs,
				largestCache:  deviceLargestCache(dev),
				repairRounds:  repair,
				frontierInput: input,
				hasCacheID:    hasCacheID,
			})

			capacity, ok := dev.Capacity[capacityName]
			if !ok || capacity.RequestPolicy == nil || capacity.RequestPolicy.ValidRange == nil {
				continue
			}
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
	return devices, policy
}

type informerSliceLister struct {
	informer cache.SharedIndexInformer
}

const sliceNodeIndex = "ccxalign-node"

func newSliceLister(informer cache.SharedIndexInformer) (*informerSliceLister, error) {
	if _, exists := informer.GetIndexer().GetIndexers()[sliceNodeIndex]; !exists {
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
