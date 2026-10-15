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
confirmed member -> change workload replicas`; the lifecycle executor must
preserve this ordering. `cluster/kick` is never inferred from a Pod name or
from deployment-time `NODE_IPS`.

The side-effect-free `internal/membership` planner implements this ordering
and requires full UUIDs, unique literal-IP RAFT addresses, a healthy leader,
quorum and committed-data evidence before returning one operation. Replacement
at the same replica count first joins a new member and waits for it to catch up.
Workload scaling is permitted only after all desired members are healthy and
membership has converged. An unhealthy member can only be kicked with explicit
permanent-loss approval. `internal/upgrade` applies the corresponding version,
backup, CRD and central-mode gates before a workload rollout.

`internal/ovnadmin.CommandClient` executes fixed `cluster/status`, `cluster/leave`
and `cluster/kick` commands. It resolves abbreviated member-list IDs only against
a previously pinned full UUID/address inventory, requires consistent membership
and terms across reachable peers, and rechecks the leader after sampling. Peer
health requires application of the leader's committed fence plus RAFT connections
observed on both ends. Explicit log counters determine commit/application indexes;
the log end alone is never committed-data evidence. Invalid identities fail the
observation rather than becoming a removable offline member.

`health.NewMembershipClient` validates central workload ownership and pins the
Pod UID and container instance around each exec. The executor verifies the
backup and, for kick, permanent-loss approval against the final transport
observation. After provider readback the transport observes membership again and
rejects a changed fence, term, leader or target health. Both verifier interfaces are mandatory for their relevant action;
a reference, reason or boolean alone is insufficient. Every dispatch rechecks
the central Lease. All operations are bounded; an exec error after dispatch has
an uncertain outcome and requires fresh observation. A successful unixctl reply
starts an asynchronous change and does not authorize workload scaling.

These checks do not make the multiple OVSDB/Kubernetes observations atomic or
fence an already submitted command after Lease loss. The default reconciler does
not invoke membership mutations. It still needs lifecycle wiring for the backup
provider, durable approval verification, persistent NB/SB operation state and a new-member provisioner. There
is no online `cluster/join` unixctl primitive: `ovsdb-tool join-cluster` prepares
a new database before its process starts, and a departed server cannot rejoin.

The status phase distinguishes installing, adopting, upgrading, recovering
and degraded plans. `Ready` requires successful workload, database, runtime and
configured network verification from the current reconciliation. Missing or
failed verification keeps it false. `WorkloadsReady` reports Kubernetes rollout; only
a complete observed rollout updates `observedVersion`. A serialized plan alone
never populates ready replica counts, peer state or the observed version.
The `KubeOVNRecoveryRequest` API records an auditable human decision and does
not perform a database wipe automatically.

## Backup capture contract

The backup adapter streams native `ovsdb-client backup` from the same owned,
pinned Pod/container used for RAFT observation. It requires installation-bound
Leases and a healthy quorum, and records an unchanged database identity,
membership, term, leader and committed/applied fence across capture. Native
backups contain a standalone schema and data transaction, not the RAFT IDs or
index. Continuous writes may prevent this conservative capture from completing;
there is no claim of an atomic NB/SB checkpoint or an application write barrier.

The initial store writes private files beneath an existing persistent root,
fsyncs the data, metadata and directories, then publishes by atomic rename.
A SHA-256 reference binds metadata to the snapshot digest. Capture and every
membership verification reopen and stream the stored data. Native lengths,
SHA-1 framing, JSON/schema shape, SHA-256, installation/database identities,
the committed fence and an explicit age/size policy must all pass. Schema,
token and row-count limits bound parsing memory. Truncated data, exec/disk
failure, cancellation and stale process identity cannot yield a usable reference.

The store is a trusted, operator-owned boundary; content hashes are not human
approval signatures. No published backup is automatically deleted. The default
controller does not provision a volume or connect this provider to the CR yet.
S3, retention/scheduling, NB/SB checkpoint coordination, RPO/RTO policy and the
restore/recovery executor remain part of the full lifecycle goal. Native OVSDB
restore tests verify schema/data loading and preserved row UUIDs, but runtime
format validation does not reimplement the OVSDB semantic type checker.

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
2. Wire the guarded unixctl membership transport into a persistent NB/SB
   lifecycle executor, with stable workload identities, the backup provider,
   durable approvals and a new-member provisioner. Validate node replacement and every
   intermediate quorum state before enabling automatic changes.
3. Complete backup storage/policy and lifecycle execution providers and verify complete Kube-OVN
   installation/transition networking. A missing health provider keeps
   `Ready=False`. Add transport-level fencing for
   remote operations already in flight when a Lease is lost.
4. Add HCP peer transport using Secret references only. Private key material
   must never be copied into a CR, status field or ConfigMap.
