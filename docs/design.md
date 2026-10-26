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
permanent-loss approval. `internal/upgrade` defines the corresponding version,
backup, CRD and central-mode contract; the reconciler enforces the
version-only backup checkpoint and pauses a requested upgrade when
`spec.upgrade.pauseOnDegraded` is true and the retained installation is
degraded. A target upgrade resumes through durable `CRDMigration`,
`CentralRollout`, `DataPlaneRollout` and `Verification` phases. CRDs are
observed as Established and NamesAccepted before central resources are
applied. Central resources must pass workload, OVN database and northd
readiness before DaemonSets are applied. A lower target persists a
`status.upgrade.rollback` record with the source/target versions, backup pair
and topology binding when available, measured RPO, and the durable execution
phase. When the native restore and workload adapters expose all required
capabilities, the controller sets `status.upgrade.canRollback` and advances
Quiescing, Restoring, Workloads and Verifying under both Leases. The rollback
contract also requires a digest-pinned target Chart, an atomic restore fence,
and explicit lost-quorum approval when quorum evidence is unavailable.
The workload rollback adapter persists each owned controller's original
`paused` or `updateStrategy` value in a rollback annotation before quiescing.
It restores every live object carrying that marker after the target Chart is
applied, including controllers removed from the target manifest, and refuses
to overwrite a marker belonging to another operation. The marker token is a
SHA-256 binding of the immutable rollback or recovery plan, so reusing a
backup pair in a later request cannot adopt the earlier request's pause state.
If database restore fails after quiescing, the execution records a terminal
failure and leaves those workloads quiesced for explicit operator recovery;
automatic unquiescing is unsafe while the database state is uncertain.
The rollback renderer also preserves the retained Chart Deployment replica
count after independent central-member handoff and re-adds the configured
network-probe objects, so restoring an older Chart cannot duplicate RAFT
members or silently remove its verification workload.

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

These read-only observations do not make the multiple OVSDB/Kubernetes samples
atomic. Mutating OVN transport uses the explicit `ovnadmin.FencedRunner`; when
the configured remote fence service is unavailable, the operator fails closed
instead of falling back to a cancellable `pods/exec` request. The default
reconciler now resumes an already persisted operation before Chart rendering and creates a
successor topology baseline only after the immutable completion receipt and fresh
final observation pass. `internal/lifecycle` connects the backup, approval and
member providers with persistent joint NB/SB operation state. The default
controller admits one safe one-step reduction and consumes a single
lifecycle-authorized apply checkpoint after the successor baseline is pinned.
The checkpoint retains a second hash with `central.memberRequests` removed, so
later explicit retirement or replacement requests can update only lifecycle
inputs and the central replica count; version, chart, placement, storage and
network changes still require a fresh lifecycle authorization.
Explicit additions are selected one at a time from pinned member requests;
the ordinary Chart Deployment handoff is now explicit and UID-bound. Broader
automatic multi-step changes, node lifecycle handling and generation transitions
remain open.
There is no online `cluster/join` unixctl primitive: `ovsdb-tool join-cluster` prepares
a new database before its process starts, and a departed server cannot rejoin in
place. Recovery `rejoin` therefore means an explicit prepared replacement
member admitted through the joint lifecycle journal.

`internal/prepare` now implements explicit local NB/SB preparation: persist the
installation/operation/member-bound plan, create native join files, validate and
sync both, then record their generated server identities. Retrying after partial
preparation or after joining preserves those identities; damaged or mismatched
files are retained with an error. It does not start processes or prove remote
quorum. `/member-run` now validates and holds those identities while supervising
explicit NB/SB servers and northd, with inherited locks and parent-death cleanup.
Northd uses full client inventories and pinned cluster UUIDs. The Kubernetes
workload provisioner must persist those identities and authorize startup; it
must not invoke upstream automatic bootstrap/recovery paths. See
[preparation and launcher requirements](member-preparation.md).
The [launcher contract](member-launch.md) defines process/storage ownership.
The [Kubernetes workload adapter](member-workloads.md) now provides the durable
Job/result/StatefulSet handoff with pinned installation, Node and volume
identities. The [joint lifecycle executor](joint-lifecycle.md) now performs
joint convergence and exact single-member StatefulSet retirement, plus explicit
Chart Deployment Pod retirement and replacement. It can infer one lost
same-size replacement target when a UID-bound approval is present; broader
default-controller Deployment migration and automatic node lifecycle handling
remain open.

The status phase distinguishes installing, adopting, upgrading, recovering
and degraded plans. `Ready` requires successful workload, database, runtime and
configured network verification from the current reconciliation. Missing or
failed verification keeps it false. `WorkloadsReady` reports Kubernetes rollout; only
a complete observed rollout updates `observedVersion`. A serialized plan alone
never populates ready replica counts, peer state or the observed version.
The `KubeOVNRecoveryRequest` API resolves and pins a namespaced installation
identity before it records an immutable recovery plan. The plan digest covers
the request generation, installation UID/generation, mode, backup reference,
reason, expiry, source/target versions, target Chart digest, both database
references and self-reported `approvedBy`; the latter is audit input and is
never proof of authority. Rejoin requests may omit a backup, while reconvert
and fresh-bootstrap requests must name the installation's verified backup pair
from `status.lastBackup` or a still-valid verified upgrade checkpoint. The
selected pair must match the observed version, configured destination and
retained topology.
Rejoin requests must name an explicit `spec.targetMemberRef` from
`central.memberRequests`; the plan also records the target-slot and retained
topology digests so a changed node, volume, image or endpoint cannot be used by
an executor after admission.
Expired, changed, recreated, HCP-targeted or backup-mismatched requests become
terminal failures. A valid request is persisted as `AwaitingExecution`.
Only an explicitly injected `RecoveryExecutor` can advance it to `Executing`
and `Succeeded`; the normal operator injects the rejoin adapter and the native
NB/SB restore adapter, while plan-only mode leaves both unset. Reconvert and
fresh-bootstrap persist the same restart-safe execution phases and use the
verified pair under the installation Leases. The independent
`KubeOVNMemberRemovalApproval` API and provider reserve an immutable decision
under the installation Leases and record completion for every approved database.
The transport rechecks that decision after final observation and bounds dispatch
by its expiry. See [approval permissions and operation flow](member-removal-approvals.md).

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
approval signatures. The lifecycle controller records a verified pair of
references, its pair ID, observed version
and retained topology digest in `status.lastBackup`, together with the
configured destination. The built-in backend accepts a local absolute `file://`
URL that exactly matches the operator's `--backup-dir`, or an
`s3://bucket/prefix` URL signed with credentials already exposed through the
AWS credential environment. S3 publication uses immutable digest-keyed objects
and returns a reference only after both snapshot and manifest uploads succeed;
partial uploads are not deleted automatically. File and S3 stores expose a
restricted retention surface that lists only complete digest-keyed pairs and
deletes only pairs in the configured destination; pending, malformed, unknown
and incomplete objects are skipped. A version-only upgrade
re-verifies a matching pair or captures a new pair and persists a separate
`status.upgrade.checkpoint`. Its `Capturing` phase is written before either
native capture, `Verified` is written only after both references and both
post-capture fences pass, and `Applying` is persisted before the Chart write.
`Applied` records an accepted write and `Failed` records a terminal safety
failure. The checkpoint binds both immutable references to the source version,
target version, destination and retained topology, so `status.lastBackup` cannot
replace or authorize a different target. Checkpoint consumers recompute the
content-addressed pair ID and require a non-zero capture time before accepting
`Verified`, `Applying` or `Applied` state after a restart. This is a durable cross-database
coordination protocol; the two native backup calls are still sequential and do
not provide an application write barrier or an atomic RAFT snapshot.
`spec.upgrade.backup.schedule` accepts an interval duration between one minute
and thirty days. Once due, the controller captures a verified NB/SB pair under
both operation Leases, using the retained topology and observed version as the
binding. The scheduler persists `status.lastBackup` and exits before Chart
rendering; it never changes `status.applyCheckpoint` or grants apply
authorization. It skips HCP/data-plane-only installations, active lifecycle
operations, version-only upgrades and installations without retained topology.
`spec.upgrade.backup.retention.keepLast` and `maxAge` enable cleanup; empty
fields disable deletion. Cleanup runs under both operation Leases, protects
the latest verified pair, the upgrade checkpoint, active lifecycle journal
references and non-terminal `KubeOVNRecoveryRequest` plan references, and
records failures in `status.backupRetention` for retry. Cleanup is independent
of the next scheduled capture deadline. S3 pair deletion retries a partial
object deletion idempotently, so a transient failure between the two DELETE
requests does not strand the remaining object.
RPO is recorded as the age of the verified pair at rollback or destructive
recovery admission. RTO is recorded only after a version-aware executor
verifies the restored workloads and both databases. Native restore uses
`ovsdb-client restore` on the pinned central process, rechecks cluster
membership around each database, and stops on a lost Lease or topology change.
The lifecycle manager also requires an explicit atomic restore-fence adapter;
ordinary Lease checks cannot authorize a native command that may already be in
flight. The repository includes the remote restore-fence client, reference
`fence-agent` and Helm sidecar wiring. Production rollback and destructive
recovery remain blocked until that agent is deployed with mTLS and its
takeover, membership and restore behavior passes a multi-node acceptance test.
Non-destructive rejoin now reuses the joint lifecycle journal after explicit
target admission. Native OVSDB restore tests verify schema/data loading
and preserved row UUIDs, but runtime format validation does not reimplement the
OVSDB semantic type checker.

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

When `spec.adoption.enabled` is true, the controller uses the same workload
adapter's adoption seam before the regular apply. A non-empty, validated label
selector is required. All chart resources in the adoption set must already
exist, match the selector, and have stable UIDs/resource versions. Deleting
objects, foreign controller owners, multiple owners, and installation labels
belonging to another installation are rejected. The adapter dry-runs every
metadata patch before any write, checks the installation and central Leases
before each write, and claims only operator labels and the installation
controller owner; workload fields remain owned by their existing managers.
Generated network-probe objects are intentionally outside this set because they
are created by the following regular apply. An adoption request therefore
cannot silently create a replacement or force ownership of an existing field.

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
The default binary publishes a same-cluster Kubernetes peer provider's
non-sensitive peer identity and endpoint evidence through the `HCPReady`
condition and `status.hcp`; cross-cluster deployments can inject a provider
with the same contract.

1. Complete configurable network values, chart-revision-specific target RBAC
   review and live installation/network acceptance. The repository roles now
   cover the operator's current exec, lifecycle and common chart-resource
   paths, but the exact upstream chart resource set and cluster admission
   policy still need an end-to-end check. The renderer verifies HTTPS archive or OCI
   chart-layer SHA-256, chart appVersion and kubeVersion, maps upstream values,
   and separates lifecycle hooks from normal resources.
2. Connect the persistent joint lifecycle executor to default-controller operation
   selection, lifecycle-authorized topology transitions and generation/apply-checkpoint changes.
   The default controller now retains and verifies the initial complete local RAFT
   identity inventory and admits explicit `central.memberRequests` additions one
   at a time while retaining the Chart Deployment count; these requests are
   accepted only after that initial retained topology exists. A request can also
   set `replaceEndpoint` to replace one pinned independent StatefulSet or Chart
   Deployment Pod at the same replica count; a reduction can set
   `retireEndpoint` for an exact Chart Deployment Pod; see [central
   topology](central-topology.md). An explicit replacement, or an
   approval-backed request that infers one unique healthy target, may acknowledge
   stale RAFT or client `InternalIP` data only for that exact retained Pod/Node
   identity; the controller refreshes the target addresses and preparation
   remotes before admission. If a Chart Deployment Pod has already disappeared
   with a failed Node, the same explicit request may use a UID-pinned
   `removalApprovalRef` to authorize a permanent-loss kick and retire one pinned
   ReplicaSet replacement Pod. If the endpoint is omitted, the selector may
   infer it only when exactly one retained Pod has disappeared, changed UID, or
   has the narrowly scoped healthy-node address drift described above; ambiguity,
   a transient Node condition and any non-target identity/address change remain
   blocked. Broader automatic node lifecycle handling and Chart Deployment
   migration to independently controlled members remain open. Validate every
   intermediate quorum state before enabling automatic changes. A referenced
   permanent-loss approval supplies the lifecycle request's operation UUID, so
   an approval can be created before the replacement request without binding to
   an unknown random identifier.
3. Complete backup storage/policy and lifecycle execution providers and verify complete Kube-OVN
   installation/transition networking. A missing health provider keeps
   `Ready=False`. Mutating OVN transport now requires the explicit
   `ovnadmin.FencedRunner` capability and a `lease.FenceToken` carrying the
   Lease UID, holder and transition epoch; a plain `pods/exec` runner fails
   closed. The Helm renderer can inject the digest-pinned mTLS `fence-agent`
   sidecar, its OVN socket and certificate mounts, and an installation-scoped
   Lease reader RoleBinding. `{node}` endpoint templates resolve the live Pod
   or Node IP for host-network central workloads. A production cluster test
   still has to prove that the deployed agent atomically validates the token
   before an operation already in flight can be fenced, and that the same
   path works for restore input.
4. Complete HCP peer transport behind the `internal/hcp.PeerProvider`
   boundary. The default binary now provides a same-cluster Kubernetes
   implementation: it requires reciprocal `KubeOVNInstallation` references,
   matching role/version and observed peer workloads, then loads the
   generation-pinned TLS Secret in memory and probes both endpoints. The
   boundary accepts only an installation identity, version, HCP configuration
   and non-sensitive metadata for the referenced TLS Secret (namespace, name,
   UID and resource version), and returns non-sensitive peer/health evidence.
   Cross-cluster transport and real two-cluster HCP E2E remain open. A report
   that claims pairing or readiness must echo the exact Secret generation from
   the request. Secret replacement or rotation therefore invalidates the
   previous report and requires a fresh observation. Private key material must
   never be copied into a CR, status field or ConfigMap. For data-plane `ssl:`
   endpoints, the chart-mounted Secret must use the native Kube-OVN
   `cacert`/`cert`/`key` keys. Offline rendering removes the upstream live-lookup
   Secret template and rewrites its fixed `kube-ovn-tls` references to the
   admitted `tlsSecretRef` name.
