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

Device requests infer shape from count and capacity with the driver's published `requestPolicy`
rounding applied. For allocated claims, shape and placement are read from the allocation result.

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

## Configuration

The plugin accepts one argument configuring the scoring strategy:
```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: dracpu-scheduler
    pluginConfig:
      - name: CCXAlign
        args:
          scoringStrategy:
            type: MostAllocated # or LeastAllocated
```
