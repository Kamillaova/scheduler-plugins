# Scheduler-plugins as a second scheduler in cluster

## Table of Contents

<!-- toc -->
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Installing the chart](#installing-the-chart)
    - [Install chart using Helm v3.0+](#install-chart-using-helm-v30)
    - [Verify that scheduler and plugin-controller pod are running properly.](#verify-that-scheduler-and-plugin-controller-pod-are-running-properly)
  - [Configuration](#configuration)
- [VM Eviction Protection](#vm-eviction-protection)
<!-- /toc -->

## Installation

Quick start instructions for the setup and configuration of as-a-second-scheduler using Helm.

### Prerequisites

- [Helm](https://helm.sh/docs/intro/quickstart/#install-helm)

### Installing the chart

#### Install chart using Helm v3.0+

> 🆕 Starting v0.28, Helm charts are hosted on https://scheduler-plugins.sigs.k8s.io

```bash
$ git clone git@github.com:kubernetes-sigs/scheduler-plugins.git
$ cd scheduler-plugins/manifests/install/charts
$ helm install --repo https://scheduler-plugins.sigs.k8s.io scheduler-plugins scheduler-plugins
```

#### Verify that scheduler and plugin-controller pod are running properly.

```bash
$ kubectl get deploy -n scheduler-plugins
NAME                           READY   UP-TO-DATE   AVAILABLE   AGE
scheduler-plugins-controller   1/1     1            1           7s
scheduler-plugins-scheduler    1/1     1            1           7s
```

### Configuration

The following table lists the configurable parameters of the as-a-second-scheduler chart and their default values.

| Parameter                      | Description                  | Default                                                                                         |
|--------------------------------|------------------------------|-------------------------------------------------------------------------------------------------|
| `scheduler.name`               | Scheduler name               | `scheduler-plugins-scheduler`                                                                   |
| `scheduler.image`              | Scheduler image              | `registry.k8s.io/scheduler-plugins/kube-scheduler:v0.35.7`                                      |
| `scheduler.command`            | Scheduler command            | `["/bin/kube-scheduler"]`                                                                       |
| `scheduler.leaderElect`        | Scheduler leaderElection     | `true`                                                                                          |
| `scheduler.replicaCount`       | Scheduler replicaCount       | `1`                                                                                             |
| `scheduler.priorityClassName`  | Scheduler priorityClassName  | `""`                                                                                            |
| `scheduler.resources`          | Scheduler resources          | `{}`                                                                                            |
| `scheduler.nodeSelector`       | Scheduler nodeSelector       | `{}`                                                                                            |
| `scheduler.affinity`           | Scheduler affinity           | `{}`                                                                                            |
| `scheduler.tolerations`        | Scheduler tolerations        | `[]`                                                                                            |
| `controller.name`              | Controller name              | `scheduler-plugins-controller`                                                                  |
| `controller.image`             | Controller image             | `registry.k8s.io/scheduler-plugins/controller:v0.35.7`                                          |
| `controller.replicaCount`      | Controller replicaCount      | `1`                                                                                             |
| `controller.priorityClassName` | Controller priorityClassName | `""`                                                                                            |
| `controller.resources`         | Controller resources         | `{}`                                                                                            |
| `controller.nodeSelector`      | Controller nodeSelector      | `{}`                                                                                            |
| `controller.affinity`          | Controller affinity          | `{}`                                                                                            |
| `controller.tolerations`       | Controller tolerations       | `[]`                                                                                            |
| `plugins.enabled`              | Plugins enabled by default   | `["Coscheduling","CapacityScheduling","NodeResourceTopologyMatch", "NodeResourcesAllocatable"]` |
| `plugins.disabled`             | Plugins disabled by default  | `["PrioritySort"]`                                                                              |
| `percentageOfNodesToScore`     | Percentage of nodes to score | `null`                                                                                          |
| `priorityClass.enabled`        | Deploy VM PriorityClass      | `false`                                                                                         |
| `priorityClass.name`           | PriorityClass name           | `vm-priority`                                                                                   |
| `priorityClass.value`          | PriorityClass value          | `100000000`                                                                                     |

## VM Eviction Protection

Virtual machine workloads require protection across multiple control-plane layers against involuntary preemption, eviction, and node drainage:

1. **Scheduler Preemption**:
   The chart can ship a dedicated `PriorityClass` with `preemptionPolicy: Never` (`priorityClass.enabled: true`). Configured with a priority value above ordinary workloads (e.g. `100000000`), VM pods are scheduled ahead of lower-priority pods and cannot be selected as preemption victims. Because `preemptionPolicy` is `Never`, VM pods also never preempt other pods.

2. **Kubelet Node-Pressure Eviction (Quality of Service)**:
   VM pods should mirror their total DRA claim capacities into pod-level `resources.requests` and `resources.limits` for both CPU and memory (`requests.cpu == limits.cpu` and `requests.memory == limits.memory`). This places the pod in the Kubernetes `Guaranteed` Quality-of-Service (QoS) tier, ensuring the kubelet assigns an `oom_score_adj` of -997. Under node memory or disk pressure, `Guaranteed` pods are evicted last, only after `BestEffort` and `Burstable` workloads, and only if the pod exceeds its own limits.

3. **Voluntary Eviction, Node Drain, and Deschedulers**:
   Each VM pod should be protected by a `PodDisruptionBudget` (PDB) specifying `maxUnavailable: 0` (or `minAvailable: 1`). Voluntary eviction operations (such as `kubectl drain` and descheduler evictions) respect PDBs and will not evict a running VM pod without explicit eviction handling or live migration.

4. **Taint-Based Eviction on Node Problems**:
   VM pods should declare tolerations for node problem condition taints without `tolerationSeconds`:
   - `node.kubernetes.io/not-ready:Exists`
   - `node.kubernetes.io/unreachable:Exists`
   Omitting `tolerationSeconds` prevents the kubelet and taint-manager from evicting VM pods during transient node heartbeat delays or temporary network partitions.

