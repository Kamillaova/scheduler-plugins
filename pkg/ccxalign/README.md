# CCXAlign

CCXAlign scores nodes by whether a [dra.cpu](https://github.com/kubernetes-sigs/dra-driver-cpu)
CPU claim can land aligned to uncore caches (AMD CCX / L3 domains) there.

## Why

A grouped dra.cpu device is one NUMA node's CPUs with a consumable capacity.
The scheduler sees its free capacity as a scalar: 32 free CPUs scattered two
per cache and 32 forming two whole caches are the same number. A whole-cache
claim can therefore be bound to a node that can never align it while a
neighbour could — and a bound claim never moves to another node.

The driver publishes the shape on a node annotation (`dra.cpu/fit`): per NUMA
node, the CPUs each cache can hold, the CPUs free now, and the CPUs free after
the node's defragmenter has repacked what it is actually willing to move.

## How it scores

For each of the pod's unallocated dra.cpu claims, largest first, against the
NUMA node the allocator's first-fit search will pick (device order from the
driver's ResourceSlice, requests rounded by its request policy):

| tier | meaning |
| --- | --- |
| 100 | fits on the fewest caches its size allows, right now |
| 70  | fits that way after the node's defragmenter repacks |
| 20  | fits only split — and also any node whose shape is unknown |

Minus a small penalty when the placement would consume a whole cache beyond
what the claim inherently needs, so small claims prefer already-dirty nodes
and whole caches are kept for whole-cache claims. The node is worth its worst
claim.

Score-only by design: fragmentation is mutable state, and filtering on it
flaps pods Unschedulable. When no node has an aligned home, the pod still
schedules and node-local defragmentation repairs what it can.

Between allocating a claim and the driver's next publish, the claim is
reserved against the node in memory, so a burst of whole-cache pods does not
pile onto one stale annotation. A reservation dies with its claim, with its
pod's failed binding, or after a short TTL.

## Requirements

- The dra.cpu driver deployed with `publishFitAnnotation: true`.
- This scheduler run with `--feature-gates=DRAConsumableCapacity=true`.
- Kubernetes ≥ 1.34 apiserver with the same gate enabled.

See `manifests/ccxalign/scheduler-config.yaml` for a profile example, and the
`as-a-second-scheduler` chart's `plugins.score` and `scheduler.extraArgs`
values for deploying it.
