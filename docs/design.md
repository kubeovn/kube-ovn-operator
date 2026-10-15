# Implementation contract

This repository is the first implementation slice for
[kubeovn/kube-ovn#7682](https://github.com/kubeovn/kube-ovn/issues/7682).

`KubeOVNInstallation` is the single declarative input. The reconciler validates
the version contract, installation mode, central topology, backup policy and
HCP pairing before writing a plan. The plan ConfigMap is deliberately a stable
adapter boundary for the Kube-OVN Helm renderer and the OVN central admin
client. It contains no credentials and no shell commands.

Every workload operation holds an installation Lease. RAFT control-plane
operations also hold a separate central-membership Lease. Each acquisition uses
a unique holder, optimistic concurrency and an uncached client; renewal failure
cancels the operation and every workload mutation rechecks the Lease. Contenders
use locally observed expiry to tolerate wall-clock skew. Release cannot clear a
replacement holder. Plan-only reconciliation does not acquire these Leases.
The membership actions are ordered as
`validate quorum -> join replacement -> wait for committed data -> leave/kick
confirmed member -> change workload replicas`; a future admin adapter must
preserve this ordering. `cluster/kick` is never inferred from a Pod name or
from deployment-time `NODE_IPS`.

The side-effect-free `internal/membership` planner implements this ordering
and requires a cluster ID, a majority of healthy members, and committed-data
evidence before it returns a join, leave, or kick operation. An unhealthy
member can only be kicked when the caller supplies explicit permanent-loss
approval. `internal/upgrade` applies the corresponding version, backup, CRD,
and central-mode gates before a workload rollout. `internal/chart` renders the
same contract as Helm values, while `internal/ovnadmin.Executor` re-reads
quorum, identity, and commit indexes immediately before invoking an injected
OVNDB transport, and requires the central operation Lease for each mutation.

The status phase distinguishes installing, adopting, upgrading, recovering
and degraded plans. `Ready` requires successful workload, database, runtime and
configured network verification from the current reconciliation. Missing or
failed verification keeps it false. `WorkloadsReady` reports Kubernetes rollout; only
a complete observed rollout updates `observedVersion`. A serialized plan alone
never populates ready replica counts, peer state or the observed version.
The `KubeOVNRecoveryRequest` API records an auditable human decision and does
not perform a database wipe automatically.

## Kubernetes workload apply contract

`internal/apply.Client` is a dynamic-client boundary for rendered workload
objects. Every object must have a fixed name and a namespace matching the
installation. The adapter stamps these labels and, for namespaced objects, a
single controller owner reference (except retained/PVC resources):

* `app.kubernetes.io/managed-by=kube-ovn-operator`
* `operator.kubeovn.io/installation=<name>`
* `operator.kubeovn.io/installation-namespace=<namespace>`
* `operator.kubeovn.io/installation-uid=<UID>`

Before any mutation, the adapter re-reads every existing object, rejects a
different owner or installation identity, and sends server-side dry-run
requests for the complete batch. New objects also pass a server-side create
dry-run before the real create. Only after all preflight requests succeed does
it persist objects; existing objects use server-side apply without force
ownership. This gives a failed dry-run an all-or-nothing guarantee for the
batch's persistent requests while preserving Kubernetes retry semantics for a
later write race. A reconciler can inject one adapter per installation through
`WorkloadFactory`. The main program wires both the Helm renderer and dynamic
apply client by default. `--plan-only` explicitly selects values-only rendering.
Before any workload write, a status checkpoint pins the spec and manifest
digests. Reconciliation rejects changed inputs until a lifecycle executor can
authorize the transition. PVCs and resources with Helm keep policy have no
owner reference and survive installation deletion.

## Next adapter boundaries

The initial read-only OVN health provider queries each owned central pod's
`_Server.Database` metadata through bounded `pods/exec` requests. `OVNDBReady`
requires matching NB/SB cluster identities, unique member IDs, client-visible
data, storage connectivity and a single leader per database; standalone mode
checks its own model. These sampled observations are separate from membership
mutation preflight and from `NetworkReady`, which uses separate pod canaries.
Failed observations and incomplete workload rollouts invalidate old evidence.
`NorthdReady` samples active/standby state and both NB/SB connections.
`DataPlaneReady` checks every scheduled OVS node for SB connectivity, a live
integration bridge and a unique chassis ID. Pod/controller ownership, container
restarts, Node identity/readiness and DaemonSet rollout are checked around the
sample. These read-only queries do not invoke the upstream health script,
which can reset SB connection state. They neither prove packet delivery nor
authorize membership changes. Production read/exec permissions require the
same deployment review as the full chart.

An explicit `spec.networkProbe.image` adds two ordinary-pod DaemonSets and a
Service to the initial apply/checkpoint. Scheduling copies the rendered OVS
constraints, without copying privileges, credentials or host networking. The
network provider requires complete coverage of the observed OVS nodes and all
pod/Service address families. It checks same-node and cross-node ring Pod IPs,
Service IPs and DNS with fresh nonces, strict response identities and bounded
HTTP requests. It rejects stale pod/container/node identities or changed
Service/DaemonSet configuration. Checks run with at most eight concurrent exec
requests and a 60-second observation deadline. Timeout leaves the network
unready; partial coverage is never promoted to success. These are sampled HTTP
and DNS checks, not a complete data-plane conformance suite. Control-plane-only
installations skip them; external/HCP verification still awaits peer evidence.

1. Extend the Helm SDK archive renderer with OCI support, configurable network
   values, complete target RBAC and live installation/network acceptance. The
   initial renderer verifies archive SHA-256, chart appVersion and kubeVersion,
   maps upstream values, and separates lifecycle hooks from normal resources.
2. Supply an OVNDB transport (libovsdb or an authenticated admin sidecar) that
   implements the `internal/ovnadmin.Client` interface and executes one
   membership operation under the central Lease.
3. Add backup and lifecycle execution providers and verify complete Kube-OVN
   installation/transition networking. A missing health provider keeps
   `Ready=False`. Add transport-level fencing for
   remote operations already in flight when a Lease is lost.
4. Add HCP peer transport using Secret references only. Private key material
   must never be copied into a CR, status field or ConfigMap.
