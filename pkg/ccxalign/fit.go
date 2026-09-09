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
	"fmt"
	"sort"
)

// FitAnnotation is the node annotation the dra.cpu driver publishes with the
// shape of each NUMA node's free CPUs. Device capacity alone cannot carry it:
// 32 free CPUs scattered two per cache and 32 forming two whole caches are the
// same scalar to the scheduler.
const FitAnnotation = "dra.cpu/fit"

// fitVersion is the only payload version this plugin understands. A node
// announcing anything else is scored as if it published nothing.
const fitVersion = 1

// Placement policies the driver may be configured with. They decide which of
// the caches that can hold a claim it is placed in, and the plugin's placement
// simulation has to follow the deployed one or its damage estimate describes a
// node the driver would never produce.
const (
	policyPack   = "pack"
	policySpread = "spread"
)

// fitReport is the annotation payload.
type fitReport struct {
	V      int       `json:"v"`
	Policy string    `json:"policy,omitempty"`
	NUMA   []numaFit `json:"numaNodes"`
}

// numaFit describes one NUMA node's uncore caches with three index-aligned
// arrays: the CPUs each cache can hold (net of the reserved set and any static
// shared pool), the CPUs free in it now, and the CPUs that would be free in it
// once the driver's defragmenter has repacked what it is actually willing to
// move.
type numaFit struct {
	ID               int   `json:"id"`
	CacheCPUs        []int `json:"cacheCPUs"`
	FreeCPUs         []int `json:"freeCPUs"`
	RepackedFreeCPUs []int `json:"repackedFreeCPUs"`
}

func (n numaFit) clone() numaFit {
	n.FreeCPUs = append([]int(nil), n.FreeCPUs...)
	n.RepackedFreeCPUs = append([]int(nil), n.RepackedFreeCPUs...)
	return n
}

// parseFit reads and validates an annotation. Every reason to reject one is a
// reason to score the node as unknown rather than to fail: a plugin that
// errors here would reject the pod on every node in the cluster, so a single
// hand-edited or newer-versioned annotation must not be able to do that.
func parseFit(raw string) (*fitReport, error) {
	var report fitReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return nil, err
	}
	if report.V != fitVersion {
		return nil, fmt.Errorf("unsupported fit annotation version %d", report.V)
	}
	if len(report.NUMA) == 0 {
		return nil, fmt.Errorf("no NUMA nodes")
	}
	switch report.Policy {
	case "", policyPack, policySpread:
	default:
		return nil, fmt.Errorf("unknown placement policy %q", report.Policy)
	}
	for _, numa := range report.NUMA {
		if len(numa.CacheCPUs) == 0 {
			return nil, fmt.Errorf("NUMA node %d lists no caches", numa.ID)
		}
		if len(numa.FreeCPUs) != len(numa.CacheCPUs) || len(numa.RepackedFreeCPUs) != len(numa.CacheCPUs) {
			return nil, fmt.Errorf("NUMA node %d arrays disagree on the cache count", numa.ID)
		}
		freeSum, repackedSum := 0, 0
		for i, capacity := range numa.CacheCPUs {
			if capacity < 0 || numa.FreeCPUs[i] < 0 || numa.RepackedFreeCPUs[i] < 0 {
				return nil, fmt.Errorf("NUMA node %d cache %d has negative CPUs", numa.ID, i)
			}
			if numa.FreeCPUs[i] > capacity || numa.RepackedFreeCPUs[i] > capacity {
				return nil, fmt.Errorf("NUMA node %d cache %d reports more free CPUs than it holds", numa.ID, i)
			}
			freeSum += numa.FreeCPUs[i]
			repackedSum += numa.RepackedFreeCPUs[i]
		}
		// A repack moves claims without resizing them, and can only reunite
		// CPUs a split core had made unusable, so it never frees fewer.
		if freeSum > repackedSum {
			return nil, fmt.Errorf("NUMA node %d frees fewer CPUs after a repack (%d) than before (%d)", numa.ID, repackedSum, freeSum)
		}
	}
	return &report, nil
}

// spreadOver is the fewest caches whose listed CPUs can hold need, or 0 when
// they cannot hold it at all. Largest-first is exact because a claim may take
// any subset of a cache.
func spreadOver(caches []int, need int) int {
	if need <= 0 {
		return 0
	}
	sorted := append([]int(nil), caches...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
	spread := 0
	for _, free := range sorted {
		if need <= 0 || free <= 0 {
			break
		}
		need -= free
		spread++
	}
	if need > 0 {
		return 0
	}
	return spread
}

func sum(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

// place simulates the driver's own selector taking need CPUs and returns what
// the free array looks like afterwards. Under "spread" the driver fills the
// least-tenanted cache that can hold the claim, breaking ties on the most free
// CPUs; under "pack" the fullest that can hold it. Neither can place a claim no
// single cache holds, so both then drain the largest caches, which is what the
// driver does too.
func place(capacity, free []int, need int, policy string) []int {
	out := append([]int(nil), free...)
	if need <= 0 || len(out) == 0 {
		return out
	}
	for need > 0 {
		best := -1
		for i, remaining := range out {
			if remaining < need {
				continue
			}
			if best < 0 {
				best = i
				continue
			}
			if policy == policySpread {
				tenants, bestTenants := capacity[i]-out[i], capacity[best]-out[best]
				if tenants != bestTenants {
					if tenants < bestTenants {
						best = i
					}
					continue
				}
				if remaining > out[best] {
					best = i
				}
				continue
			}
			if remaining < out[best] {
				best = i
			}
		}
		if best >= 0 {
			out[best] -= need
			return out
		}

		largest := 0
		for i, remaining := range out {
			if remaining > out[largest] {
				largest = i
			}
		}
		if out[largest] <= 0 {
			// Nothing left to take. The caller asked for more than the node
			// has, which only stale data can produce.
			return out
		}
		need -= out[largest]
		out[largest] = 0
	}
	return out
}

// wholeCaches counts the caches nothing is using.
func wholeCaches(capacity, free []int) int {
	whole := 0
	for i := range capacity {
		if capacity[i] > 0 && free[i] == capacity[i] {
			whole++
		}
	}
	return whole
}

// inherentWholeCaches is how many caches a claim of this size must consume
// entirely wherever it lands: a whole-cache claim eating whole caches is the
// intended use, not damage.
func inherentWholeCaches(capacity []int, need int) int {
	sorted := append([]int(nil), capacity...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
	whole := 0
	for _, size := range sorted {
		if size <= 0 || need < size {
			break
		}
		need -= size
		whole++
	}
	return whole
}

const (
	// scoreAlignedNow is a node that can hold the claim on the fewest caches
	// its size allows, right now.
	scoreAlignedNow = 100
	// scoreAlignedRepacked is a node that can once the defragmenter has moved
	// what it is willing to move.
	scoreAlignedRepacked = 70
	// scoreSplit is a node that can hold the claim only across more caches
	// than its size requires. It is also what an unknown node scores: a node
	// that publishes nothing is uninformative, not bad, and ranking it below
	// every known-split node would blacklist a fleet mid-upgrade.
	scoreSplit = 20
	// damagePenalty is charged once when placing the claim would consume a
	// cache no whole-cache claim can then use. It is deliberately smaller than
	// the gap between tiers, so it can only order nodes within one.
	damagePenalty = 5
)

// scoreNUMA ranks how well need CPUs would land on one NUMA node, and reports
// whether they fit there at all.
func scoreNUMA(numa numaFit, need int, policy string) (int64, bool) {
	minSpread := spreadOver(numa.CacheCPUs, need)
	if minSpread == 0 {
		// Larger than this NUMA node can ever hold.
		return 0, false
	}

	nowSpread := spreadOver(numa.FreeCPUs, need)
	repackedSpread := spreadOver(numa.RepackedFreeCPUs, need)

	tier := 0
	view := numa.FreeCPUs
	switch {
	case nowSpread == minSpread:
		tier = scoreAlignedNow
	case repackedSpread == minSpread:
		// Checked even when the claim does not fit at all right now: under
		// whole-core allocation a repack reunites split cores, so a node can
		// be unable to hold the claim now and able to hold it aligned after
		// the defragmenter runs. That node is exactly what defragmentation
		// exists for.
		tier, view = scoreAlignedRepacked, numa.RepackedFreeCPUs
	case nowSpread > 0:
		tier = scoreSplit
	case repackedSpread > 0:
		tier, view = scoreSplit, numa.RepackedFreeCPUs
	default:
		return 0, false
	}

	// Damage is measured in the view that decided the tier: for a node that
	// only fits after a repack, what the claim does to today's free CPUs is
	// undone by the repack anyway.
	destroyed := wholeCaches(numa.CacheCPUs, view) - wholeCaches(numa.CacheCPUs, place(numa.CacheCPUs, view, need, policy))
	if destroyed-inherentWholeCaches(numa.CacheCPUs, need) > 0 {
		tier -= damagePenalty
	}
	return int64(tier), true
}

// scoreNode ranks a node for every claim the pod holds.
//
// The claims are walked largest first against the NUMA node the driver's
// allocator would pick for each -- the first published device that can hold it,
// which is how the structured allocator's first-fit search runs -- subtracting
// each claim as it goes, so a pod holding two claims is not scored as if both
// could have the same cache. The node is worth its worst claim: a pod is only
// as aligned as its least aligned claim.
//
// numaOrder is the NUMA node ids in the order the driver published their
// devices. Nodes whose shape is unknown score scoreSplit, so a node missing
// from the report is neither preferred nor blacklisted.
func scoreNode(report *fitReport, numaOrder []int, needs []int, policy string) int64 {
	byID := make(map[int]numaFit, len(report.NUMA))
	for _, numa := range report.NUMA {
		byID[numa.ID] = numa.clone()
	}

	sorted := append([]int(nil), needs...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))

	worst := int64(scoreAlignedNow)
	for _, need := range sorted {
		scored := false
		for _, numaID := range numaOrder {
			numa, ok := byID[numaID]
			if !ok || sum(numa.FreeCPUs) < need {
				continue
			}
			score, fits := scoreNUMA(numa, need, policy)
			if !fits {
				continue
			}
			if score < worst {
				worst = score
			}
			numa.FreeCPUs = place(numa.CacheCPUs, numa.FreeCPUs, need, policy)
			numa.RepackedFreeCPUs = place(numa.CacheCPUs, numa.RepackedFreeCPUs, need, policy)
			byID[numaID] = numa
			scored = true
			break
		}
		if !scored {
			// No NUMA node has room, which the scheduler's own filter says is
			// impossible -- so the report is stale. Say nothing rather than
			// something wrong, but never say something better than a claim
			// already scored: the node is worth its worst claim, and a later
			// claim falling off the end of stale data does not redeem an
			// earlier one that genuinely fits badly.
			return min(worst, scoreSplit)
		}
	}
	return worst
}
