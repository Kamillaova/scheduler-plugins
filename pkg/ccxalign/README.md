# CCXAlign

CCXAlign scores nodes by whether a [dra.cpu](https://github.com/kubernetes-sigs/dra-driver-cpu)
CPU claim can land aligned to uncore caches (AMD CCX / L3 domains) there.

## Why

A grouped dra.cpu device is an uncore cache or NUMA node's CPUs with consumable capacity.
The scheduler sees free capacity as a scalar, which cannot distinguish whether free CPUs
form whole caches or are scattered across them. CCXAlign steers claims to nodes where
they fit on the fewest caches their sizes allow, right now or after defragmentation.

The allocation state comes directly from the scheduler's DRA manager (`SharedDRAManager`),
including in-flight allocations signaled earlier in the scheduling cycle. The plugin keeps
no separate reservations overlay and does not simulate the driver's defragmentation planner.

## How it scores

Pods without claims resolving to `dra.cpu` via the DeviceClass informer are a no-op.
When a pod holds `dra.cpu` claims, they are evaluated jointly as an overlay in allocation
order (`pod.Spec.ResourceClaims` order), and the pod receives the minimum tier across its claims.

Shape is inferred from the request that names a device class of this driver -- not from the claim's
first request, which may belong to another driver -- using its count and capacity with the driver's
published `requestPolicy` rounding applied. For allocated claims, shape and placement are read from
the allocation result, matched on pool and device name together, since device names repeat across
nodes and only the pool tells them apart.

A cache's size comes from the `dra.cpu/largestUncoreCacheCPUs` topology attribute, taking the largest
cache in the claim's partition. It is deliberately not the published capacity: the driver rewrites
capacity to mirror claims that have left a cache or squat on one, so that value moves with
defragmentation while the cache itself does not, and it is floored on a squatted device.

### Codebook

Scores are computed on a fixed absolute scale without normalisation:

| Tier | Packing (`MostAllocated`) | Spreading (`LeastAllocated`) |
| --- | --- | --- |
| Warm-aligned | 80 + bonus (80..88) | 80 + bonus (80..88) |
| Repairable (1 round) | 60 + bonus (60..68) | 60 + bonus (60..68) |
| Repairable (2 rounds) | 40 + bonus (40..48) | 40 + bonus (40..48) |
| Cold-aligned | 40 + bonus (40..48) | 80 + bonus (80..88) |
| Repairable (3 rounds) | 20 + bonus (20..28) | 20 + bonus (20..28) |
| Split | 0 + bonus (0..8) | 0 + bonus (0..8) |

Cold means no claim of this driver is allocated on the node within the claim's partition.

### Incremental cache footprint bonus (0..8)

- **Packing**:
  - Sub-cache (< 1 cache): 8 when reusing an occupied cache, 0 when opening a clean cache, 4 when landing is unknown.
  - Multi-cache: count of occupied caches in the landing NUMA node within the partition (capped at 8).
- **Spreading**:
  - Count of clean caches in the landing NUMA node within the partition for every shape (capped at 8).

### Partition scoping

Tiers, bonus and warm/cold status are evaluated strictly over devices matching the claim's
partition (`dra.cpu/partition`, defaulting to `default`). A node running only a dataplane
daemon in another partition is cold for workloads in the default partition.

### Audit figure and metrics

The landing NUMA node predicted from slice order is recorded in cycle state as an audit
figure for comparison against actual allocation in tests and telemetry.

Prometheus metrics:
- `ccxalign_chosen_tier_total`: counter labeled by `tier` (`warm-aligned`, `repairable`, `cold-aligned`, `split`).
- `ccxalign_predicted_vs_actual_mismatches_total`: counter labeled by `reason`.
- `ccxalign_fail_closed_total`: counter labeled by `reason`.

## How it filters

Filter applies to unallocated claims requesting `alignment: Repairable` (`v1alpha1.AlignmentRepairable`).

A pod's claims are evaluated against one overlay per partition, shared across the pod's claims, so a
later claim sees what an earlier one debited: two whole-cache claims in one pod are not both admitted
to a node holding one clean cache.

1. **Foreign reservations:** Rejects any claim carrying another pod's reservation (`claim.Status.ReservedFor[].UID != pod.UID`) before any node evaluation. An allocated claim's pinned node is never rejected.
2. **Alignment without alternatives:** A claim that asks for `Repairable` while offering no split alternative is rejected on every node, before anything about the node is considered. The allocator has only the aligned shape to choose from, so such a claim is never split and never needs repair; asking for it is a producer error. The aligned sub-cache shapes the driver's documentation recommends -- a single `exactly` request below one cache -- must therefore carry no `alignment` at all.
3. **Aligned fits now:** If the aligned alternative fits immediately on any NUMA node within the claim's partition (`cleanCachesInNUMA >= k`), Filter passes without inspecting repair frontiers.
4. **Universal feasibility and freshness:** Otherwise, Filter passes only if, on every NUMA node where the claim's first feasible split alternative fits within the claim's partition, the repair frontier for size $k$ is reachable (`1 | 2 | 3` rounds) and fresh. Freshness requires that `dra.cpu/frontierInput` matches the plugin ledger digest (`api.FrontierInputDigest`) and no in-flight allocation of the current cycle has landed on that NUMA node. Missing, stale, or unknown-version frontiers fail closed on this branch. Predecessor attribute `dra.cpu/repairMoves` is accepted as fallback.
5. **Cache grouping requirement:** `Repairable` claims require fine-grained cache grouping (`dra.cpu/cacheL3ID`). Coarser groupings (`numanode`, `socket`, `machine`) fail closed.
6. **Requeue events:** Implements `EnqueueExtensions` registering `ResourceSlice` (`Add | Update`), `Node` (`Add | Update`) and `ResourceClaim` (`Add | Update | Delete`), so a pod rejected on a frontier wakes when the driver republishes it and a pod rejected on claim state wakes when that claim changes.

## Configuration

An example profile configuration (`manifests/ccxalign/scheduler-config.yaml`) configures CCXAlign with scoring strategy, weights, and disabled scorers:
```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: true
profiles:
  - schedulerName: dracpu-scheduler
    percentageOfNodesToScore: 100
    plugins:
      multiPoint:
        enabled:
          - name: CCXAlign
      score:
        enabled:
          - name: CCXAlign
            weight: 10
        disabled:
          - name: ImageLocality
          - name: PodTopologySpread
          - name: DynamicResources
    pluginConfig:
      - name: NodeResourcesFit
        args:
          scoringStrategy:
            type: MostAllocated
```
