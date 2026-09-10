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
	"strconv"
	"strings"

	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured"
)

type claimShape string

const (
	shapeNeverSplit claimShape = api.ShapeNeverSplit
	shapeFlexible   claimShape = api.ShapeFlexible
)

type tierRank int

const (
	rankSplit       tierRank = 1
	rankRepairable3 tierRank = 2
	rankColdAligned tierRank = 3
	rankRepairable2 tierRank = 3
	rankRepairable1 tierRank = 4
	rankWarmAligned tierRank = 5
)

type claimEvaluation struct {
	baseScore    int
	bonus        int
	totalScore   int
	tierLabel    string
	tierRank     tierRank
	landingNUMA  int
	landingFound bool
}

type nodeOverlayDevice struct {
	name          string
	poolName      string
	numaNodeID    int
	cacheID       string
	partition     string
	totalCPUs     int
	consumedCPUs  int
	freeCPUs      int
	repairRounds  string
	frontierInput string
	hasCacheID    bool
}

func getClaimPartition(claim *resourceapi.ResourceClaim) string {
	for _, req := range claim.Spec.Devices.Requests {
		if req.Exactly != nil {
			for _, tol := range req.Exactly.Tolerations {
				if tol.Key == partitionAttr && tol.Value != "" {
					return tol.Value
				}
			}
			for _, sel := range req.Exactly.Selectors {
				if strings.Contains(sel.CEL.Expression, "partition") {
					if part := extractPartitionFromCEL(sel.CEL.Expression); part != "" {
						return part
					}
				}
			}
		}
		for _, sub := range req.FirstAvailable {
			for _, tol := range sub.Tolerations {
				if tol.Key == partitionAttr && tol.Value != "" {
					return tol.Value
				}
			}
			for _, sel := range sub.Selectors {
				if strings.Contains(sel.CEL.Expression, "partition") {
					if part := extractPartitionFromCEL(sel.CEL.Expression); part != "" {
						return part
					}
				}
			}
		}
	}
	return defaultPartition
}

func extractPartitionFromCEL(expr string) string {
	idx := strings.Index(expr, "partition")
	if idx < 0 {
		return ""
	}
	sub := expr[idx:]
	eqIdx := strings.Index(sub, "==")
	if eqIdx < 0 {
		return ""
	}
	rem := strings.TrimSpace(sub[eqIdx+2:])
	if len(rem) < 2 {
		return ""
	}
	quote := rem[0]
	if quote != '"' && quote != '\'' {
		return ""
	}
	endQuote := strings.IndexByte(rem[1:], quote)
	if endQuote < 0 {
		return ""
	}
	return rem[1 : 1+endQuote]
}

func inferClaimShape(claim *resourceapi.ResourceClaim) claimShape {
	for _, req := range claim.Spec.Devices.Requests {
		if len(req.FirstAvailable) > 1 {
			return shapeFlexible
		}
	}
	return shapeNeverSplit
}

func parseRepairRounds(raw string, k int) int {
	if raw == "" || k < 1 || k > api.RepairRoundsFields {
		return -1
	}
	fields := strings.Split(raw, ",")
	if len(fields) < api.RepairRoundsFields {
		failClosedTotal.WithLabelValues("invalid_repair_rounds_fields").Inc()
		return -1
	}
	field := strings.TrimSpace(fields[k-1])
	if field == "" || field == "-" || field == api.RepairRoundsUnreachable {
		return -1
	}
	rounds, err := strconv.Atoi(field)
	if err != nil || rounds < 1 || rounds > 3 {
		failClosedTotal.WithLabelValues("invalid_repair_rounds_value").Inc()
		return -1
	}
	return rounds
}

type overlayMap struct {
	devices []*nodeOverlayDevice
	byNUMA  map[int][]*nodeOverlayDevice
}

func newOverlayMap(rawDevices []deviceInfo, allocatedState *structured.AllocatedState, partition string) (*overlayMap, bool) {
	om := &overlayMap{
		byNUMA: make(map[int][]*nodeOverlayDevice),
	}
	isCold := true

	for _, d := range rawDevices {
		if d.partition != partition {
			continue
		}
		consumed := 0
		if allocatedState != nil {
			devID := structured.MakeDeviceID(driverName, d.poolName, d.name)
			if allocatedState.AllocatedDevices.Has(devID) {
				consumed = d.totalCPUs
			} else if capMap, ok := allocatedState.AggregatedCapacity[devID]; ok {
				if qty, found := capMap[capacityName]; found && qty != nil {
					consumed = int(qty.Value())
				}
			}
		}
		if consumed > 0 {
			isCold = false
		}
		free := d.totalCPUs - consumed
		if free < 0 {
			free = 0
		}
		dev := &nodeOverlayDevice{
			name:          d.name,
			poolName:      d.poolName,
			numaNodeID:    d.numaNodeID,
			cacheID:       d.cacheID,
			partition:     d.partition,
			totalCPUs:     d.totalCPUs,
			consumedCPUs:  consumed,
			freeCPUs:      free,
			repairRounds:  d.repairRounds,
			frontierInput: d.frontierInput,
			hasCacheID:    d.hasCacheID,
		}
		om.devices = append(om.devices, dev)
		om.byNUMA[d.numaNodeID] = append(om.byNUMA[d.numaNodeID], dev)
	}

	return om, isCold
}

func (om *overlayMap) cacheSize() int {
	for _, d := range om.devices {
		if d.totalCPUs > 0 {
			return d.totalCPUs
		}
	}
	return defaultCacheSize
}

func (om *overlayMap) cleanCachesInNUMA(numaID int, cacheSize int) int {
	count := 0
	for _, d := range om.byNUMA[numaID] {
		if d.freeCPUs >= cacheSize && d.consumedCPUs == 0 {
			count++
		}
	}
	return count
}

func (om *overlayMap) occupiedCachesInNUMA(numaID int) int {
	count := 0
	for _, d := range om.byNUMA[numaID] {
		if d.consumedCPUs > 0 {
			count++
		}
	}
	return count
}

func (om *overlayMap) freeCPUsInNUMA(numaID int) int {
	total := 0
	for _, d := range om.byNUMA[numaID] {
		total += d.freeCPUs
	}
	return total
}

func (om *overlayMap) numaOrder() []int {
	var order []int
	seen := make(map[int]bool)
	for _, d := range om.devices {
		if !seen[d.numaNodeID] {
			seen[d.numaNodeID] = true
			order = append(order, d.numaNodeID)
		}
	}
	return order
}

func evaluateSingleClaim(
	claim *resourceapi.ResourceClaim,
	om *overlayMap,
	policy nodePolicy,
	isCold bool,
	mode ScoringStrategyType,
) claimEvaluation {
	if len(om.devices) == 0 {
		return claimEvaluation{
			baseScore:    TierSplit,
			bonus:        0,
			totalScore:   TierSplit,
			tierLabel:    TierLabelSplit,
			tierRank:     rankSplit,
			landingFound: false,
		}
	}

	cacheSize := om.cacheSize()
	numaOrder := om.numaOrder()

	if claim.Status.Allocation != nil {
		allocatedHere := false
		var landingNUMA int
		allWholeCaches := true
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver != driverName {
				continue
			}
			for _, d := range om.devices {
				if d.name == result.Device {
					allocatedHere = true
					landingNUMA = d.numaNodeID
					break
				}
			}
		}
		if !allocatedHere {
			return claimEvaluation{
				baseScore:    TierSplit,
				bonus:        0,
				totalScore:   TierSplit,
				tierLabel:    TierLabelSplit,
				tierRank:     rankSplit,
				landingFound: false,
			}
		}
		if allWholeCaches {
			bonus := 0
			if mode == Spreading {
				bonus = min(om.cleanCachesInNUMA(landingNUMA, cacheSize), MaxBonus)
				return claimEvaluation{
					baseScore:    TierWarmAligned,
					bonus:        bonus,
					totalScore:   TierWarmAligned + bonus,
					tierLabel:    TierLabelWarmAligned,
					tierRank:     rankWarmAligned,
					landingNUMA:  landingNUMA,
					landingFound: true,
				}
			}
			if isCold {
				bonus = min(om.occupiedCachesInNUMA(landingNUMA), MaxBonus)
				return claimEvaluation{
					baseScore:    TierColdAligned,
					bonus:        bonus,
					totalScore:   TierColdAligned + bonus,
					tierLabel:    TierLabelColdAligned,
					tierRank:     rankColdAligned,
					landingNUMA:  landingNUMA,
					landingFound: true,
				}
			}
			bonus = min(om.occupiedCachesInNUMA(landingNUMA), MaxBonus)
			return claimEvaluation{
				baseScore:    TierWarmAligned,
				bonus:        bonus,
				totalScore:   TierWarmAligned + bonus,
				tierLabel:    TierLabelWarmAligned,
				tierRank:     rankWarmAligned,
				landingNUMA:  landingNUMA,
				landingFound: true,
			}
		}
		return claimEvaluation{
			baseScore:    TierSplit,
			bonus:        0,
			totalScore:   TierSplit,
			tierLabel:    TierLabelSplit,
			tierRank:     rankSplit,
			landingNUMA:  landingNUMA,
			landingFound: true,
		}
	}

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
	isSubCache := totalDemand < cacheSize && count == 1

	if isSubCache {
		var landingDev *nodeOverlayDevice
		for _, dev := range om.devices {
			if dev.freeCPUs >= roundedDemand {
				landingDev = dev
				break
			}
		}

		if landingDev != nil {
			landingNUMA := landingDev.numaNodeID
			bonus := 0
			if mode == Spreading {
				bonus = min(om.cleanCachesInNUMA(landingNUMA, cacheSize), MaxBonus)
			} else {
				if landingDev.consumedCPUs > 0 {
					bonus = MaxBonus
				} else {
					bonus = 0
				}
			}

			landingDev.consumedCPUs += roundedDemand
			landingDev.freeCPUs -= roundedDemand
			if landingDev.freeCPUs < 0 {
				landingDev.freeCPUs = 0
			}

			if mode == Spreading {
				return claimEvaluation{
					baseScore:    TierWarmAligned,
					bonus:        bonus,
					totalScore:   TierWarmAligned + bonus,
					tierLabel:    TierLabelWarmAligned,
					tierRank:     rankWarmAligned,
					landingNUMA:  landingNUMA,
					landingFound: true,
				}
			}

			if isCold {
				return claimEvaluation{
					baseScore:    TierColdAligned,
					bonus:        bonus,
					totalScore:   TierColdAligned + bonus,
					tierLabel:    TierLabelColdAligned,
					tierRank:     rankColdAligned,
					landingNUMA:  landingNUMA,
					landingFound: true,
				}
			}

			return claimEvaluation{
				baseScore:    TierWarmAligned,
				bonus:        bonus,
				totalScore:   TierWarmAligned + bonus,
				tierLabel:    TierLabelWarmAligned,
				tierRank:     rankWarmAligned,
				landingNUMA:  landingNUMA,
				landingFound: true,
			}
		}

		bonus := 4
		if mode == Spreading {
			bonus = 0
			if len(numaOrder) > 0 {
				bonus = min(om.cleanCachesInNUMA(numaOrder[0], cacheSize), MaxBonus)
			}
		}
		return claimEvaluation{
			baseScore:    TierSplit,
			bonus:        bonus,
			totalScore:   TierSplit + bonus,
			tierLabel:    TierLabelSplit,
			tierRank:     rankSplit,
			landingFound: false,
		}
	}

	k := (totalDemand + cacheSize - 1) / cacheSize
	if k < 1 {
		k = 1
	}

	var alignedLandingNUMA int
	alignedFitsNow := false
	for _, numaID := range numaOrder {
		if om.cleanCachesInNUMA(numaID, cacheSize) >= k {
			alignedLandingNUMA = numaID
			alignedFitsNow = true
			break
		}
	}

	if alignedFitsNow {
		bonus := 0
		if mode == Spreading {
			bonus = min(om.cleanCachesInNUMA(alignedLandingNUMA, cacheSize), MaxBonus)
		} else {
			bonus = min(om.occupiedCachesInNUMA(alignedLandingNUMA), MaxBonus)
		}

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

		if mode == Spreading {
			return claimEvaluation{
				baseScore:    TierWarmAligned,
				bonus:        bonus,
				totalScore:   TierWarmAligned + bonus,
				tierLabel:    TierLabelWarmAligned,
				tierRank:     rankWarmAligned,
				landingNUMA:  alignedLandingNUMA,
				landingFound: true,
			}
		}

		if isCold {
			return claimEvaluation{
				baseScore:    TierColdAligned,
				bonus:        bonus,
				totalScore:   TierColdAligned + bonus,
				tierLabel:    TierLabelColdAligned,
				tierRank:     rankColdAligned,
				landingNUMA:  alignedLandingNUMA,
				landingFound: true,
			}
		}

		return claimEvaluation{
			baseScore:    TierWarmAligned,
			bonus:        bonus,
			totalScore:   TierWarmAligned + bonus,
			tierLabel:    TierLabelWarmAligned,
			tierRank:     rankWarmAligned,
			landingNUMA:  alignedLandingNUMA,
			landingFound: true,
		}
	}

	shape := inferClaimShape(claim)
	if shape == shapeFlexible && firstReq != nil && len(firstReq.FirstAvailable) > 1 {
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
			splitDemand := subCount * subCPUs

			var feasibleNUMAs []int
			for _, numaID := range numaOrder {
				if om.freeCPUsInNUMA(numaID) >= splitDemand {
					feasibleNUMAs = append(feasibleNUMAs, numaID)
				}
			}

			if len(feasibleNUMAs) == 0 {
				continue
			}

			allReachable := true
			worstRounds := 0
			for _, numaID := range feasibleNUMAs {
				numaDevs := om.byNUMA[numaID]
				repairRaw := ""
				for _, d := range numaDevs {
					if d.repairRounds != "" {
						repairRaw = d.repairRounds
						break
					}
				}
				rounds := parseRepairRounds(repairRaw, k)
				if rounds < 1 || rounds > 3 {
					allReachable = false
					break
				}
				if rounds > worstRounds {
					worstRounds = rounds
				}
			}

			landingNUMA := feasibleNUMAs[0]
			bonus := 0
			if mode == Spreading {
				bonus = min(om.cleanCachesInNUMA(landingNUMA, cacheSize), MaxBonus)
			} else {
				bonus = min(om.occupiedCachesInNUMA(landingNUMA), MaxBonus)
			}

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

			if allReachable && worstRounds >= 1 && worstRounds <= 3 {
				baseScore := 80 - 20*worstRounds
				rank := rankRepairable1
				if worstRounds == 2 {
					rank = rankRepairable2
				} else if worstRounds == 3 {
					rank = rankRepairable3
				}
				return claimEvaluation{
					baseScore:    baseScore,
					bonus:        bonus,
					totalScore:   baseScore + bonus,
					tierLabel:    TierLabelRepairable,
					tierRank:     rank,
					landingNUMA:  landingNUMA,
					landingFound: true,
				}
			}

			return claimEvaluation{
				baseScore:    TierSplit,
				bonus:        bonus,
				totalScore:   TierSplit + bonus,
				tierLabel:    TierLabelSplit,
				tierRank:     rankSplit,
				landingNUMA:  landingNUMA,
				landingFound: true,
			}
		}
	}

	bonus := 0
	if len(numaOrder) > 0 {
		if mode == Spreading {
			bonus = min(om.cleanCachesInNUMA(numaOrder[0], cacheSize), MaxBonus)
		} else {
			bonus = min(om.occupiedCachesInNUMA(numaOrder[0]), MaxBonus)
		}
	}
	return claimEvaluation{
		baseScore:    TierSplit,
		bonus:        bonus,
		totalScore:   TierSplit + bonus,
		tierLabel:    TierLabelSplit,
		tierRank:     rankSplit,
		landingFound: false,
	}
}

func scorePodOnNode(
	claims []*resourceapi.ResourceClaim,
	rawDevices []deviceInfo,
	allocatedState *structured.AllocatedState,
	policy nodePolicy,
	mode ScoringStrategyType,
) (int64, string, int) {
	if len(claims) == 0 {
		return TierSplit, TierLabelSplit, -1
	}

	switch mode {
	case LeastAllocated, Spreading:
		mode = Spreading
	default:
		mode = Packing
	}

	claimPartition := getClaimPartition(claims[0])
	om, isCold := newOverlayMap(rawDevices, allocatedState, claimPartition)

	worstScore := int64(100)
	worstTierRank := rankWarmAligned
	worstTierLabel := TierLabelWarmAligned
	predictedLanding := -1

	for _, claim := range claims {
		eval := evaluateSingleClaim(claim, om, policy, isCold, mode)
		if eval.landingFound && predictedLanding == -1 {
			predictedLanding = eval.landingNUMA
		}
		if eval.tierRank < worstTierRank || worstScore == 100 {
			worstTierRank = eval.tierRank
			worstTierLabel = eval.tierLabel
			worstScore = int64(eval.totalScore)
		} else if eval.tierRank == worstTierRank && int64(eval.totalScore) < worstScore {
			worstScore = int64(eval.totalScore)
		}
	}

	if worstScore < 0 {
		worstScore = 0
	} else if worstScore > 100 {
		worstScore = 100
	}

	return worstScore, worstTierLabel, predictedLanding
}

func ScoreTestClaim(
	claims []*resourceapi.ResourceClaim,
	devices []resourceapi.Device,
	occupiedDevs []string,
	mode ScoringStrategyType,
) (int64, string, int) {
	var rawDevices []deviceInfo
	var policy nodePolicy
	adopted := false

	for _, dev := range devices {
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

		rawDevices = append(rawDevices, deviceInfo{
			name:          dev.Name,
			poolName:      "cpu-pool",
			numaNodeID:    numaID,
			cacheID:       cacheID,
			partition:     part,
			totalCPUs:     totalCPUs,
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

	var allocatedState *structured.AllocatedState
	if len(occupiedDevs) > 0 {
		occupiedSet := make(map[string]struct{}, len(occupiedDevs))
		for _, name := range occupiedDevs {
			occupiedSet[name] = struct{}{}
		}
		allocCollection := make(structured.ConsumedCapacityCollection)
		for _, dev := range devices {
			if _, ok := occupiedSet[dev.Name]; ok {
				devCap := 16
				if capVal, ok := dev.Capacity[capacityName]; ok {
					devCap = int(capVal.Value.Value())
				}
				consumed := devCap / 2
				if consumed < 1 {
					consumed = 1
				}
				devID := structured.MakeDeviceID(driverName, "cpu-pool", dev.Name)
				allocCollection[devID] = structured.ConsumedCapacity{
					capacityName: resource.NewQuantity(int64(consumed), resource.DecimalSI),
				}
			}
		}
		allocatedState = &structured.AllocatedState{
			AllocatedDevices:   sets.New[structured.DeviceID](),
			AggregatedCapacity: allocCollection,
		}
	}

	return scorePodOnNode(claims, rawDevices, allocatedState, policy, mode)
}
