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
	"testing"
)

func TestParseFitRejectsWhatWouldMisleadOrCrash(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"not json", `{`},
		{"newer version", `{"v":2,"numaNodes":[{"id":0,"cacheCPUs":[16],"freeCPUs":[16],"repackedFreeCPUs":[16]}]}`},
		{"no numa nodes", `{"v":1,"numaNodes":[]}`},
		{"empty cache arrays", `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[],"freeCPUs":[],"repackedFreeCPUs":[]}]}`},
		{"mismatched arrays", `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[16,16],"freeCPUs":[16],"repackedFreeCPUs":[16,16]}]}`},
		{"negative free", `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[16],"freeCPUs":[-1],"repackedFreeCPUs":[16]}]}`},
		{"free above capacity", `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[16],"freeCPUs":[17],"repackedFreeCPUs":[16]}]}`},
		{"repack frees fewer", `{"v":1,"numaNodes":[{"id":0,"cacheCPUs":[16,16],"freeCPUs":[8,8],"repackedFreeCPUs":[8,0]}]}`},
		{"unknown policy", `{"v":1,"policy":"sprinkle","numaNodes":[{"id":0,"cacheCPUs":[16],"freeCPUs":[16],"repackedFreeCPUs":[16]}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseFit(tc.raw); err == nil {
				t.Fatalf("parseFit accepted %q", tc.raw)
			}
		})
	}
}

func TestSpreadOver(t *testing.T) {
	for _, tc := range []struct {
		name   string
		caches []int
		need   int
		want   int
	}{
		{"fits one cache", []int{16, 16}, 8, 1},
		{"exactly one cache", []int{16, 16}, 16, 1},
		{"needs two", []int{16, 16}, 24, 2},
		{"scattered free forces spread", []int{2, 2, 2, 2}, 6, 3},
		{"cannot fit", []int{2, 2}, 6, 0},
		{"zero-free caches are not counted", []int{0, 8, 0}, 8, 1},
		{"zero need", []int{8}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := spreadOver(tc.caches, tc.need); got != tc.want {
				t.Fatalf("spreadOver(%v, %d) = %d, want %d", tc.caches, tc.need, got, tc.want)
			}
		})
	}
}

func TestPlaceFollowsThePublishedPolicy(t *testing.T) {
	capacity := []int{16, 16, 16}
	free := []int{8, 16, 4} // caches 0 and 2 have tenants, cache 1 is whole

	// Pack best-fits: the 4-CPU claim goes to the fullest cache that holds it.
	packed := place(capacity, free, 4, policyPack)
	if packed[2] != 0 {
		t.Fatalf("pack must fill the fullest fitting cache, got %v", packed)
	}
	// Spread picks the least-tenanted cache: the whole one.
	spread := place(capacity, free, 4, policySpread)
	if spread[1] != 12 {
		t.Fatalf("spread must open the least-tenanted cache, got %v", spread)
	}
	// A claim no cache holds drains the largest first under either policy.
	big := place(capacity, free, 20, policySpread)
	if big[1] != 0 || sum(big) != sum(free)-20 {
		t.Fatalf("an over-cache claim must drain the largest cache first, got %v", big)
	}
	// Stale data asking for more than exists must not loop or panic.
	drained := place(capacity, free, 100, policyPack)
	if sum(drained) != 0 {
		t.Fatalf("draining everything must terminate, got %v", drained)
	}
	if got := place(nil, nil, 4, policyPack); len(got) != 0 {
		t.Fatalf("empty arrays must be a no-op, got %v", got)
	}
}

func TestScoreNUMATiers(t *testing.T) {
	// The confetti shape: every cache hosts a small tenant, and repacking
	// would empty two caches for a two-cache claim.
	confetti := numaFit{
		CacheCPUs:        []int{16, 16, 16, 16},
		FreeCPUs:         []int{14, 14, 14, 14},
		RepackedFreeCPUs: []int{16, 16, 12, 12},
	}
	if got, ok := scoreNUMA(confetti, 32, policySpread); !ok || got != scoreAlignedRepacked {
		t.Fatalf("32 CPUs on confetti: got %d/%v, want the aligned-after-repack tier", got, ok)
	}

	clean := numaFit{
		CacheCPUs:        []int{16, 16, 16, 16},
		FreeCPUs:         []int{16, 16, 14, 14},
		RepackedFreeCPUs: []int{16, 16, 14, 14},
	}
	if got, ok := scoreNUMA(clean, 32, policySpread); !ok || got != scoreAlignedNow {
		t.Fatalf("32 CPUs on two whole caches: got %d/%v, want aligned-now with no damage", got, ok)
	}

	// Irreducible: 16 CPUs can never be aligned on caches free 6 apiece, even
	// repacked.
	irreducible := numaFit{
		CacheCPUs:        []int{16, 16, 16, 16},
		FreeCPUs:         []int{6, 6, 6, 6},
		RepackedFreeCPUs: []int{6, 6, 6, 6},
	}
	if got, ok := scoreNUMA(irreducible, 16, policySpread); !ok || got != scoreSplit {
		t.Fatalf("irreducible split: got %d/%v, want the split tier", got, ok)
	}

	// Whole-core reunification: nothing fits now, but the repack frees the
	// split cores and the claim then fits aligned. This is the node
	// defragmentation exists for; it must not score as unusable.
	reunify := numaFit{
		CacheCPUs:        []int{16, 16},
		FreeCPUs:         []int{6, 8},
		RepackedFreeCPUs: []int{16, 2},
	}
	if got, ok := scoreNUMA(reunify, 16, policySpread); !ok || got != scoreAlignedRepacked {
		t.Fatalf("repack-only fit: got %d/%v, want the aligned-after-repack tier", got, ok)
	}

	if _, ok := scoreNUMA(clean, 100, policySpread); ok {
		t.Fatal("a claim larger than the NUMA node must not fit")
	}
}

func TestScoreNUMADamage(t *testing.T) {
	// A 4-CPU claim: a node that must break its only whole cache pays the
	// penalty, a node with no whole cache to break does not -- across nodes,
	// not within one, is where the term steers.
	mustBreak := numaFit{
		CacheCPUs:        []int{16, 16},
		FreeCPUs:         []int{16, 2},
		RepackedFreeCPUs: []int{16, 2},
	}
	nothingToBreak := numaFit{
		CacheCPUs:        []int{16, 16},
		FreeCPUs:         []int{8, 8},
		RepackedFreeCPUs: []int{8, 8},
	}
	broken, _ := scoreNUMA(mustBreak, 4, policySpread)
	kept, _ := scoreNUMA(nothingToBreak, 4, policySpread)
	if broken != scoreAlignedNow-damagePenalty || kept != scoreAlignedNow {
		t.Fatalf("damage must charge only the node losing a whole cache: broke=%d kept=%d", broken, kept)
	}

	// A whole-cache claim consuming whole caches is use, not damage.
	twoWhole := numaFit{
		CacheCPUs:        []int{16, 16, 16},
		FreeCPUs:         []int{16, 16, 4},
		RepackedFreeCPUs: []int{16, 16, 4},
	}
	if got, _ := scoreNUMA(twoWhole, 32, policySpread); got != scoreAlignedNow {
		t.Fatalf("inherent whole-cache use must not be charged: got %d", got)
	}

	// Damage for a repack-tier node is judged in the repacked view: what the
	// claim does to today's shape is undone by the repack anyway.
	repackTier := numaFit{
		CacheCPUs:        []int{16, 16},
		FreeCPUs:         []int{10, 10}, // whole caches now: none
		RepackedFreeCPUs: []int{16, 4},  // after repack: one, which the claim consumes
	}
	if got, _ := scoreNUMA(repackTier, 16, policySpread); got != scoreAlignedRepacked {
		t.Fatalf("consuming the repacked whole cache is inherent for a 16-CPU claim: got %d", got)
	}
}

func mustFit(t *testing.T, report fitReport) *fitReport {
	t.Helper()
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseFit(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestScoreNodeMirrorsTheAllocatorAcrossNUMANodes(t *testing.T) {
	// numa0 fits the claim by capacity but only split; numa1 holds a whole
	// cache. The allocator is first-fit in device order, so it will bind the
	// claim to numa0 -- and the score must say split, not advertise numa1's
	// alignment the claim will never get.
	report := mustFit(t, fitReport{V: 1, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16, 16}, FreeCPUs: []int{8, 8}, RepackedFreeCPUs: []int{8, 8}},
		{ID: 1, CacheCPUs: []int{16, 16}, FreeCPUs: []int{16, 2}, RepackedFreeCPUs: []int{16, 2}},
	}})
	if got := scoreNode(report, []int{0, 1}, []int{16}, policySpread); got != scoreSplit {
		t.Fatalf("first-fit mirror: want the split tier of numa0, got %d", got)
	}
	// With numa0 too full to hold it, the allocator moves on to numa1.
	report = mustFit(t, fitReport{V: 1, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16, 16}, FreeCPUs: []int{4, 4}, RepackedFreeCPUs: []int{4, 4}},
		{ID: 1, CacheCPUs: []int{16, 16}, FreeCPUs: []int{16, 2}, RepackedFreeCPUs: []int{16, 2}},
	}})
	if got := scoreNode(report, []int{0, 1}, []int{16}, policySpread); got != scoreAlignedNow {
		t.Fatalf("first NUMA that fits: want numa1's aligned-now, got %d", got)
	}
}

func TestScoreNodeIsWorthItsWorstClaim(t *testing.T) {
	// Two whole-cache claims, one whole cache: the first lands aligned, the
	// second splits. The pod is only as aligned as its least aligned claim.
	report := mustFit(t, fitReport{V: 1, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16, 16, 16}, FreeCPUs: []int{16, 8, 8}, RepackedFreeCPUs: []int{16, 8, 8}},
	}})
	if got := scoreNode(report, []int{0}, []int{16, 16}, policySpread); got != scoreSplit {
		t.Fatalf("two claims, one cache: want the second claim's split tier, got %d", got)
	}
	// With two whole caches, both align.
	report = mustFit(t, fitReport{V: 1, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16, 16, 16}, FreeCPUs: []int{16, 16, 8}, RepackedFreeCPUs: []int{16, 16, 8}},
	}})
	if got := scoreNode(report, []int{0}, []int{16, 16}, policySpread); got != scoreAlignedNow {
		t.Fatalf("two claims, two caches: want aligned-now, got %d", got)
	}
}

func TestScoreNodeStaleDataScoresNeutral(t *testing.T) {
	// The scheduler's filter said capacity fits; if the report disagrees, the
	// report is stale and the node scores unknown, never zero.
	report := mustFit(t, fitReport{V: 1, NUMA: []numaFit{
		{ID: 0, CacheCPUs: []int{16}, FreeCPUs: []int{2}, RepackedFreeCPUs: []int{2}},
	}})
	if got := scoreNode(report, []int{0}, []int{16}, policySpread); got != scoreSplit {
		t.Fatalf("stale shape: want the neutral split tier, got %d", got)
	}
}
