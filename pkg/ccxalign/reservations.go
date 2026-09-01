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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// reservations is the pessimistic overlay bridging the gap between this
// scheduler placing a claim and the node's annotation reflecting it: without
// it, a burst of whole-cache pods would all score against the same
// pre-placement shape and pile onto one node -- the NodeResourceTopology
// plugin's overreserving cache, sized to this plugin's much smaller state.
//
// A reservation dies three ways: the claim informer sees the claim deleted or
// deallocated; its pod fails to bind (Unreserve); or reservationTTL passes.
//
// Note what is NOT in that list: a pod binding successfully. Its claim stays
// allocated, so neither informer path fires, and the reservation stands until
// the TTL even though the node republished its annotation -- already counting
// that claim -- within seconds. The overlay therefore double-counts a bound
// claim for the rest of its TTL, which makes the plugin pessimistic about a
// node it has just used. That direction is the safe one: it costs a
// second-best placement, never an infeasible one. Clearing it earlier would
// mean knowing the annotation in hand was published after the reservation was
// made, which the payload carries nothing to decide.
//
// A scheduler restart loses the overlay entirely, which costs a brief window
// of optimism and self-heals the same way.
type reservations struct {
	mu      sync.Mutex
	now     func() time.Time
	byClaim map[reservationKey]*reservation
}

// reservationKey identifies one reserved DEVICE. A claim may ask for several,
// and each lands on its own device, so keying by claim UID alone would let a
// two-device claim reserve the space of one.
type reservationKey struct {
	claimUID types.UID
	device   int
}

type reservation struct {
	nodeName string
	podUID   types.UID
	cpus     int
	madeAt   time.Time
}

func newReservations() *reservations {
	return &reservations{now: time.Now, byClaim: map[reservationKey]*reservation{}}
}

func (r *reservations) add(nodeName string, podUID, claimUID types.UID, device, cpus int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byClaim[reservationKey{claimUID: claimUID, device: device}] = &reservation{
		nodeName: nodeName, podUID: podUID, cpus: cpus, madeAt: r.now(),
	}
}

func (r *reservations) removeClaim(claimUID types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.byClaim {
		if key.claimUID == claimUID {
			delete(r.byClaim, key)
		}
	}
}

func (r *reservations) removePod(podUID types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.byClaim {
		if entry.podUID == podUID {
			delete(r.byClaim, key)
		}
	}
}

// needsFor returns the still-standing reserved demands against one node,
// pruning expired entries as it goes.
func (r *reservations) needsFor(nodeName string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := r.now().Add(-reservationTTL)
	var needs []int
	for key, entry := range r.byClaim {
		if entry.madeAt.Before(cutoff) {
			delete(r.byClaim, key)
			continue
		}
		if entry.nodeName == nodeName {
			needs = append(needs, entry.cpus)
		}
	}
	return needs
}
