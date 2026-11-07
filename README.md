# Kube-OVN Operator

The Kube-OVN Operator reconciles a declarative `KubeOVNInstallation`. The
current implementation supports rendering checksum-pinned upstream Helm
charts, applying an initial resource set, and observing Kubernetes rollout.
The complete lifecycle requested in [Issue #7682](https://github.com/kubeovn/kube-ovn/issues/7682)
is still under development.

## Initial installation

The operator defaults to workload management. Set `spec.chart.source` to an
HTTPS `.tgz` URL or an `oci://` repository reference, and set
`spec.chart.digest` to the SHA-256 of the downloaded archive or OCI chart layer
(`sha256:<64 hex characters>`). The chart must contain the `kube-ovn` chart
with an `appVersion` matching `spec.version`. OCI authentication comes from the
operator process's existing Helm/Docker registry credential store; the operator
does not create, copy, or persist credentials.
HTTP is accepted only for literal loopback addresses in local integration tests.

The Helm SDK renders the original chart templates with the installation name
and namespace. It maps version, mode, external NB/SB endpoints, central storage
and the OVS update strategy to upstream values. Classic central placement uses
one label selector (default: `node-role.kubernetes.io/control-plane=`) and Node
InternalIPs. RAFT bootstrap requires exactly the requested number of addresses.
Those addresses are bootstrap input, never evidence of live RAFT membership.
`spec.dataPlane.nodeSelector` is merged into every rendered data-plane
DaemonSet and the data-plane controller/IC Deployments. Existing upstream
selectors, including the required Linux selector, are preserved; a conflicting
value is rejected before any resource is written. The chart's remaining values
use upstream defaults, including network CIDRs.

Rendering is offline: Helm `lookup` does not access the cluster. The operator
validates the target Kubernetes version, filters upgrade/delete/test hooks,
and rejects install hooks. It does not execute Helm lifecycle hooks or create
Helm release records. The requested resources must fit the operator's Kubernetes
permissions; the shipped RBAC is still the limited adapter baseline and does
not grant permission to install the upstream chart's cluster roles or CRDs.
Do not deploy a full network installation until the required RBAC and cluster
configuration have been reviewed for that environment.

Before the first workload write, status records a spec and manifest digest.
Repeated reconciliation can repair the same resource set. A changed spec or
rendered topology is blocked until a lifecycle executor can authorize it.
This checkpoint also survives an operator restart or a partially applied batch.
The default binary wires a same-cluster Kubernetes HCP peer provider before
workload apply. It finds the reciprocal `KubeOVNInstallation` in
`centralNamespace`, checks the pair role/version contract, loads the
generation-pinned TLS Secret in memory, and probes both configured endpoints.
Cross-cluster deployments can still inject another `PeerProvider`. The
`internal/hcp.PeerProvider` boundary receives only the installation identity,
version and HCP configuration (including a Secret name reference), plus the
non-sensitive Secret UID and resource version observed by the controller. A
provider must return the same Secret generation before its peer report can be
used as readiness evidence; replacing or rotating the Secret therefore forces
fresh peer observation. Secret data stays inside the provider and never enters
the request, status, plan ConfigMap or rendered chart values. The controller
exposes that evidence as `HCPReady` and `status.hcp`, and fails closed when the
provider is absent or reports an invalid pair. When HCP is enabled, the local
admission contract requires `centralNamespace`, a named `tlsSecretRef`, and
explicit OVN endpoints in `tcp:<host>:<port>` or `ssl:<host>:<port>` form. The
operator validates the namespace, Secret name, protocol, host and port without
reading or copying Secret data. A data-plane HCP installation maps these endpoints to
the upstream chart's external OVN values; the provider still owns TLS material
and remote peer coordination. A provider-side loader accepts either the
standard Kubernetes `ca.crt`/`tls.crt`/`tls.key` keys or the native Kube-OVN
`cacert`/`cert`/`key` keys, validates the bound generation and certificate
lifetime, and constructs the client configuration only in memory. For a
`dataPlaneOnly` HCP installation using `ssl:` endpoints, the Secret mounted by
the upstream Kube-OVN chart must expose the native `cacert`/`cert`/`key` keys.
The renderer removes the chart's live-lookup TLS template, rewrites its
references to `tlsSecretRef`, and preserves those native file names. Standard
Kubernetes TLS keys remain supported for provider-only or custom transport
implementations. The HCP package also provides a bounded `tcp`/`ssl` endpoint probe; it reports
transport reachability only and cannot substitute for peer role, version or
schema evidence. Dynamic membership additions use
`spec.central.memberRequests`: each request must pin a DNS member name, a Ready
node, an already Bound filesystem PVC/PV, a digest-pinned runtime image, and
the four NB/SB listener addresses. The default controller selects one unused
request at a time, records live object UIDs, prepares both native databases,
and keeps the original Chart Deployment at its retained replica count while
the independent StatefulSet converges. Replica count alone never selects a
member. These requests describe additions to an installation with a retained
topology; the initial Chart install must omit them and add them only after the
first retained topology is recorded. Set `replaceEndpoint` on a request to
replace one exact retained central Pod at the same replica count; the endpoint,
workload UID, ReplicaSet UID (for a Chart Deployment) and full retirement spec
are pinned before the replacement starts. Set `retireEndpoint` on a
retirement-only request to select one exact Chart Deployment Pod for a one-step
scale-in. The controller uses Pod deletion cost and verifies every surviving
Pod UID before accepting the Deployment handoff. For a same-size replacement
after unexpected member loss, a request may omit `replaceEndpoint` when it
carries a UID-bound `removalApprovalRef`; the selector then accepts exactly one
retained Pod that has disappeared or been replaced by a different UID. Multiple
candidates, a transient Node condition, or a still-present original Pod leave
the operation blocked. Adoption is available through an explicit selector and
only claims ownership of resources that already exist.

`internal/apply` checks ownership with uncached reads, dry-runs the complete
batch, then creates new resources or uses server-side apply without force for
existing resources. PVCs and resources annotated `helm.sh/resource-policy: keep`
receive installation labels but no owner reference, so deletion of the
installation does not garbage-collect them. Cluster-scoped resources also use
labels rather than an invalid namespaced owner.

Set `spec.adoption.enabled: true` with a non-empty Kubernetes label selector to
take over an existing installation. Every chart resource selected for adoption
must already exist in the installation namespace (or at cluster scope), match
the selector, and have no foreign controller owner, deletion timestamp, or
installation identity labels. The operator first server-side dry-runs the
complete ownership patch, then writes only its managed-by and installation
labels plus the installation controller owner under the normal Leases. Generated
network-probe resources are created by the subsequent regular apply and are not
part of the existing-resource adoption set. Adoption never deletes, recreates,
or force-claims workload fields.

## Status and verification

`WorkloadsReady` reports actual Deployment, StatefulSet and DaemonSet rollout.
It requires a current observed generation, positive desired replica/node counts,
updated and ready replicas, and matching StatefulSet revisions. Jobs, PVCs and
CRDs must also reach their relevant completion/binding/establishment states.
`observedVersion` is populated only after rollout completes.

`OVNDBReady` reports live NB/SB metadata after rollout. The operator follows
Pod controller UID links to the installation's Deployment/StatefulSet and uses
bounded, read-only `ovsdb-client query` commands through `pods/exec`. For RAFT,
all expected pods must report the same cluster ID per database, unique full
server IDs, positive client-visible data indexes, storage connectivity and
exactly one leader. Standalone mode checks the actual standalone database
model and connectivity. Failed observations clear previous health evidence.
Database identities and sampled member indexes are published in `status.topology`.

These are sampled health observations; they do not prove an atomic RAFT
membership snapshot or authorize membership changes. In particular, the
server's `connected` signal can lag a failure while an election is in progress.
External databases and HCP pairs await peer evidence; the default same-cluster
provider or an injected cross-cluster provider must report it before
`Ready=True`.

`NorthdReady` requires exactly one active northd, standby peers and NB/SB
connections on every managed central pod. `DataPlaneReady` requires a complete
OVS DaemonSet rollout, a Ready node for each pod, unique chassis UUIDs, an SB
connection from each ovn-controller and a live `br-int` in ovs-vswitchd. The
operator uses read-only appctl/vsctl commands and limits node probes to four
concurrent requests. It rechecks pod/container identities, node identities and
DaemonSet generation after sampling. Failed observations clear the affected
runtime evidence. Control-plane-only and data-plane-only installations skip
the component they do not manage.

These runtime checks do not prove end-to-end packet delivery. Enable network
verification before the first apply checkpoint with `spec.networkProbe.image`,
using an operator image containing `/network-probe` (prefer an immutable image
digest). Without this configuration, `NetworkReady` stays Unknown for an
installation with a data plane. Observation requires pod list,
Node/ReplicaSet/Deployment/StatefulSet/DaemonSet read and `pods/exec` create permissions; the
limited shipped RBAC still requires environment-specific review and expansion.

`Ready=True/VerificationComplete` requires workload, database, northd,
data-plane and network conditions to pass in the same reconciliation. Failed
or unavailable checks clear old network evidence and readiness. A rollout
awaiting checks reports `VerificationPending`; a valid plan alone reports
`PlanOnly`. Control-plane-only installations do not schedule data-plane probes.
External databases and HCP pairs still require a peer provider. Installation and RAFT
central operations acquire separate Leases with unique
operation identities. Version-only upgrades now use a durable phased rollout:
CRDs are applied and observed first, central/control-plane resources are
applied next, and DaemonSet data-plane resources are held until workload, OVN
database and northd readiness pass. The phase is stored in
`status.upgrade.step` and can resume after an operator restart. This is still
not a rollback: a target lower than `status.observedVersion` is classified as
`Rollback` and stops before backup or Chart apply. It additionally requires
`spec.upgrade.allowRollback=true`. When the production restore, workload and
fence adapters are configured, the controller executes a durable Quiescing →
Restoring → Workloads → Verifying state machine and sets
`status.upgrade.canRollback` only after all three capabilities are proven.
The blocked decision is durable in `status.upgrade.rollback`: it records the
source and target versions, the verified backup pair, retained topology
digest, target Chart digest, measured backup age as `rpoSeconds`, and the
blocking reason. A complete rollback contract additionally requires a
digest-pinned target Chart, an NB/SB restore adapter, a workload rollback
adapter, an atomic restore fence, and an explicit lost-quorum approval when
the retained quorum cannot be proven.
The production lifecycle manager exposes the atomic restore fence as an
explicit capability and refuses destructive restore when it is absent;
ordinary Kubernetes Lease checks are not a substitute. Production use
requires the separately deployed mTLS `fence-agent` (or an equivalent remote
service) to atomically admit a native restore against its current fence.
Before a destructive restore, the workload adapter records each owned
Deployment, StatefulSet and DaemonSet's original pause/update strategy in a
durable annotation while quiescing it. After the target Chart is applied,
unquiescing scans those live annotations rather than only the target manifest,
so a workload removed by the rollback is still restored and its marker is
cleared. Each marker is bound to a SHA-256 token derived from the immutable
rollback or recovery plan, rather than only the backup pair, so a later
operation cannot resume an earlier operation's pause state. A missing or
conflicting marker fails closed. Rollback rendering also
keeps the retained Chart Deployment replica count after independent central
member handoff and re-adds configured network-probe objects.

Network verification adds client and server DaemonSets using the OVS scheduling
constraints, plus a ClusterIP Service. These are unprivileged ordinary pods
without service-account tokens or host mounts. For every node and observed
IPv4/IPv6 family, the client checks a server on the same node, the next node
in a ring when multiple nodes exist, the Service IP, and its DNS name. DNS
must resolve to the observed Service address. Each HTTP response must echo a
fresh nonce and the expected Pod UID/node identity. The observer rechecks pod
restarts, addresses, nodes, DaemonSets and Service configuration after sampling.
`status.network` records node/family coverage, successful checks and observation
time. The checks cover sampled HTTP paths and Service DNS; they do not replace
all-pairs, UDP application, NetworkPolicy or full network conformance tests.
Node create, delete, label, address, readiness and schedulability events
enqueue installations whose central or data-plane placement matches the Node,
whose status retains a workload on that Node, or which has an explicit
dynamic-membership request. The retained-identity match keeps a cordon, drain,
label change or deletion visible even after the Node stops matching its current
selector. The event only shortens the observation delay; the existing topology
and Lease gates still decide whether any mutation is admitted.

The controller publishes `kubeovn_operator_reconcile_total`,
`kubeovn_operator_reconcile_duration_seconds`,
`kubeovn_operator_installation_ready` and
`kubeovn_operator_installation_condition` on the controller-runtime metrics
endpoint. The latter two use installation namespace/name labels so dashboards
can follow a specific resource; condition values are `1` for true, `0` for
false and `-1` for unknown. Phase and Ready transitions are emitted as
Kubernetes Events with the `PhaseChanged` and `ReadyChanged` reasons. Events
and metrics describe controller observations; they do not authorize a
membership or recovery mutation.

For plan inspection without fetching charts or changing workloads, run the
operator with `--plan-only`. This writes the plan ConfigMap without acquiring
operation Leases and keeps
`Ready=False`.

## Operation leases

The controller uses uncached, resourceVersion-checked Lease acquisition and
holds the installation Lease around workload changes. RAFT control-plane
writes additionally hold the central Lease, always in that order. The OVN
admin executor requires the central Lease before join/leave/kick.

Each operation has a new holder identity, renews its 30-second Lease every
10 seconds, and stops if renewal fails or its local renewal deadline passes.
Contenders must observe an unchanged Lease locally for its full duration before
taking it over; they do not trust another machine's wall-clock timestamp.
Every workload write rechecks the held scopes. Lease loss cancels the operation
context; cleanup releases only the same holder, UID and transition number.
These checks stop subsequent writes. Mutating OVN membership transport is now
fail-closed: `ovnadmin.FencedRunner` receives a `lease.FenceToken` containing
the installation UID, Lease UID, holder and transition epoch, and the remote
executor must validate that token atomically before starting the command.
Cancellation or a second local Lease check is not accepted as fencing. A plain
`Runner` can observe status but cannot dispatch leave or kick.
Set `--fence-endpoint-template` to route mutations through a trusted HTTPS
fence service; without it the standard Kubernetes `pods/exec` path remains
fail-closed. Production use also requires `--fence-ca-file`,
`--fence-client-cert-file`, and `--fence-client-key-file`; the remote
`fence-agent` requires a client certificate signed by its configured CA. The
template can contain `{namespace}`, `{pod}`, and `{container}` path
placeholders.
The protocol and restore stream are described in
[transport fencing](docs/transport-fencing.md); without a configured fence
endpoint, destructive recovery remains fail-closed.
The Helm chart can configure the same transport with `fenceAgent.enabled`.
When enabled, the renderer injects a digest-pinned `/fence-agent` sidecar into
each rendered `ovn-central` workload, mounts the configured server TLS and
client-CA Secrets, and creates a namespace-scoped Lease reader RoleBinding.
Use `{node}` in the endpoint template for host-network central Pods; the live
Pod IP is resolved before each mutation.

## Membership administration

The admin boundary reads live `cluster/status` through fixed, bounded unixctl
commands. Its inventory pins full cluster/server UUIDs and literal-IP RAFT
addresses; the four-character IDs printed in member lists never create a new
identity. Leader and peer observations must agree on membership and term.
Healthy members must have applied the leader's committed fence and have live
RAFT connections. Commit indexes use the explicit uncommitted/unapplied counters,
not the log boundary alone. A second leader read invalidates stale observations.

`health.NewMembershipClient` binds that inventory to owned central Pods and
checks Pod/container identities before and after exec. `ovnadmin.Executor`
requires the operation Leases, verifies a backup against the final observation,
and additionally verifies a durable permanent-loss approval before a kick.
Healthy members leave through their own process; a confirmed lost member is
kicked through the leader using its full server UUID. Success acknowledges the
request only: both databases must converge before workload changes.

The planner supports replacement at the same desired replica count: join one
replacement, wait for it to catch up, remove the old member, then permit workload
scaling. The command transport cannot prepare a database or start a joining
server. `internal/prepare` and `/member-prepare` now prepare a persistent pair of
native NB/SB join files and return their generated server identities.
`internal/central` and `/member-run` supervise explicit NB/SB and northd startup
from those files. `internal/workload` supplies the Kubernetes preparation and
startup adapter. `internal/lifecycle` now persists joint operation intent,
authorizes startup and observes both databases catching up before removal.
A backup capture and verification adapter is available as described below.
The durable approval adapter is described below. Default-controller operation
selection admits one addition at a time from an explicit `memberRequests` entry
and admits reductions only when the retained endpoint is either an exactly
one-member `OnDelete` StatefulSet or an explicitly selected stable Chart
Deployment Pod. If a Chart Deployment endpoint has already disappeared with a
deleted, replaced, cordoned or NotReady Node, the replacement request must also
pin a pre-created `KubeOVNMemberRemovalApproval` through
`memberRequests[].removalApprovalRef` (`name` and `uid`). The selector then pins
all retained survivors and at most one newly created ReplicaSet Pod, removes the
lost NB/SB member with the approval gate, and scales the Deployment only after
both databases converge. The lifecycle selector reuses the approval decision's
`operationID` for the durable request, so the approval can be created before the
replacement request without predicting a second random UUID. The Helm renderer
and Chart can inject the
digest-pinned mTLS `fence-agent` sidecar and the operator can use its remote
fence client. Production deployment, credential provisioning and a multi-node
takeover/restore acceptance are still required. The standard Kubernetes
`pods/exec` adapter deliberately does not claim this capability because
cancelling an already accepted exec request cannot revoke the process command.
For a same-size request with a UID-bound removal approval, the selector may
also infer one unique retained member whose Ready Node kept the same UID while
its advertised NB/SB addresses drifted. The controller verifies that only this
member's four endpoints changed, refreshes the lifecycle preparation remotes,
and then admits the canonical request; ambiguous or non-target drift remains
blocked.

## New member preparation

The Linux `/member-prepare` helper accepts a versioned plan binding installation
UID, operation UUID, stable member name and both databases' cluster UUIDs and
literal-IP RAFT addresses. It persists the immutable request, uses native
`ovsdb-tool --cid=... join-cluster`, validates both files and publishes their
generated server identities after file/directory fsync. Partial attempts reuse
already generated identities across retry. Corrupt or incompatible files are
retained with an error; existing installation databases are never reset.

The root must be an existing private directory owned by the invoking OS user,
with flock, atomic rename and fsync support. The helper uses no Kubernetes
credentials, cannot verify remote quorum and does not start servers. The
operator image includes it, but its distroless runtime has no `ovsdb-tool`;
a workload provisioner must supply a compatible native execution environment.
Default dynamic membership remains restricted to the safe selector described
above. See
[preparation persistence and startup requirements](docs/member-preparation.md).

The Linux `/member-run` launcher validates the recorded preparation result and
native identities while holding the storage lock, starts foreground NB/SB and
northd processes, and stops the entire member if any child exits. Children
inherit storage/runtime locks and terminate if the supervisor dies. Restarts
reuse the files and identities. Northd uses explicit peer client endpoints with
pinned cluster UUIDs; local follower sockets alone cannot reach both leaders.
Running processes do not prove readiness. A separate `central-runtime` Docker
target packages both helpers with a digest-pinned Kube-OVN v1.16.13 native image
and runs as UID/GID 65532. Default-controller/storage wiring, runtime image
publication/selection and lifecycle authorization are still required. See
[launch request and supervision](docs/member-launch.md).

## Member workloads

`internal/workload.Provisioner` creates a preparation Job and, only after an
explicit `Start` call with its durably recorded native result, a one-member
StatefulSet using `/member-run`. Immutable records bind the installation UID and
generation, operation, Node UID, PVC/PV UIDs, runtime image digest and workload
identities. A resourceVersion-guarded PVC reservation excludes another member.
Retries recover an accepted create response without regenerating database IDs;
recorded objects that disappear, change identity or change spec are rejected.

The caller supplies a previously provisioned, Bound filesystem PVC with a private
mount root owned by UID 65532. Preparation does not start a server. The workload
uses host networking on the pinned Node, unprivileged high ports, dropped Linux
capabilities and no service-account token. Records and storage reservations are
retained; this adapter has no delete, resize or automatic retirement path.

The default controller now resumes a persisted joint operation before rendering
or applying the Chart. It holds both operation Leases, validates the pinned
request and immutable completion receipt, and keeps ordinary Apply fenced while
the operation is incomplete. After a completed operation it discovers a fresh
final topology and creates a new immutable baseline; the old baseline remains
for audit. A safe selected reduction then authorizes the next apply checkpoint,
applies the new Chart inputs and records the handoff as complete. Explicit Chart
Deployment retirement is implemented; runtime image publication, automatic
storage provisioning and Kubernetes node replacement remain separate work. The
apply checkpoint also records a member-request-free base hash, so later explicit
retirement or replacement requests can change only lifecycle inputs and the
central replica count. Member TLS
Secret identity, key mapping and read-only mount lifecycles are pinned by the
workload request and fail closed on rotation. Pod readiness alone does not prove
database health. See
[member workload contract](docs/member-workloads.md).

Lifecycle recovery uses an existing operator-owned `--backup-dir` for local
backups, or an `s3://` destination with AWS credentials already available to
the process. `--backup-max-age` and `--backup-max-bytes` define the explicit
backup policy. The operator never creates local directories, changes their
permissions, or creates credentials.

## Retained central topology

The default reconciler discovers and retains a complete local RAFT topology
under both operation Leases after workload rollout. Full NB/SB cluster and
server UUIDs, RAFT addresses and actual client listeners are checked against
all peers. Node, workload and database storage identities are bound to the
observation. The upstream Chart's `/etc/ovn` host path and the member runtime's
`/var/lib/ovn-member` persistent claim are supported database roots.

An immutable ConfigMap without an owner reference retains the initial identity
inventory. `status.topology.inventory` pins its name, UID and digest; observation
failures do not clear it. `TopologyRecorded` is a separate readiness gate and
requires fresh discovery. Pod/container restarts can reuse the baseline only
when database, node, workload and storage identities still match. Changed native
identities or a missing/replaced record block verification, and commit-index
advancement does not create new records. See [topology discovery and retention](docs/central-topology.md).

## Joint membership lifecycle

`internal/lifecycle.Executor` persists an immutable operation request and a
resourceVersion-guarded installation journal before preparing, starting or removing
members. `NewKubernetes` connects the actual workload, pod admin, backup and
approval providers. Its inventory includes temporarily added peers, independently
of the final replica count. Both databases must catch up before either old member
can leave; both must converge before the exact old StatefulSet is scaled to zero
or the selected Chart Deployment is scaled down by one. Completion waits for the
target Pod to disappear and retains storage.

Reconstructed executors recover partial NB/SB transitions, lost responses and
the original completion receipt. A changed identity, missing retained member,
lost quorum or stale backup blocks progress. Automatic node selection and
generation transitions, production fencing deployment and broader Deployment
migration remain open. See [joint lifecycle contract](docs/joint-lifecycle.md).

## Permanent-loss approvals

`KubeOVNMemberRemovalApproval` records an explicit decision to remove one lost
member from each of one or both OVN databases. It binds the installation name
and UID, operation UUID, database/cluster/server UUIDs, literal-IP RAFT addresses,
reason and expiry. The caller pins the approval name and UID. Creation permission
is the approval authority; a self-reported `approvedBy` field is not authority.
The existing recovery request's `approvedBy` value cannot authorize a kick.

`KubeOVNRecoveryRequest` now resolves its namespaced installation reference,
pins the installation UID and generation, validates the requested mode and
expiry, and requires `backupRef` to match the installation's verified backup
pair for `reconvert` and `freshBootstrap`. The pair may come from
`status.lastBackup` or a still-valid verified upgrade checkpoint, but it must
match the observed version, destination and retained topology. It records a deterministic
identity-bound plan in status and moves valid requests to
`AwaitingExecution`. `approvedBy` is included in the plan digest for audit but
is never treated as an authorization signal. Recreated installations, changed
request generations, expired requests, HCP targets and mismatched backups are
fenced with terminal status reasons. An explicit `RecoveryExecutor` is the
only integration point that can move a plan through `Executing` to `Succeeded`.
The normal operator wires the non-destructive rejoin executor and the native
NB/SB restore executor; reconvert and fresh-bootstrap use the same durable
Quiescing → Restoring → Workloads → Verifying phases and persist their
execution phase, RPO and RTO. Plan-only mode leaves all destructive executors
unset.
Rejoin requests must also set `spec.targetMemberRef` to one named
`central.memberRequests` entry. The plan stores a digest of that complete
member slot and the retained topology, so changing its node, storage, image or
listener addresses after admission invalidates the request instead of
redirecting recovery to a different target.

`internal/approval.Provider` reserves the immutable decision in status before
dispatch, permits retries for the same operation, and records completion only
when all approved databases have quorum and no longer contain the old server.
Completed audit status is immutable and cannot authorize another dispatch.
Revocation is irreversible. Revocation and expiry block new dispatches while
still allowing an already converged operation to record completion. Provider
operations require uncached API reads and both installation-bound Leases.

The admin executor re-reads the approval after backup verification and again
after the final database observation. The approval expiry bounds the dispatch
context. Cancellation cannot retract a command already accepted by OVSDB;
uncertain results require fresh observation before retrying. This approval
reservation is not the persistent NB/SB node-lifecycle state machine.

The CRD ships with the operator. Approval roles are optional, unbound manifests
and are not installed by default. Production deployment must separate human
approval creation from the operator's status-only writes and retain Kubernetes
API audit records. The default operator account cannot create approvals and is
wired only for read and status recording. See [member removal approval
permissions](docs/member-removal-approvals.md) and [destructive recovery
approvals](docs/recovery-approvals.md).

## Database backups

`internal/backup.Provider` captures native `ovsdb-client backup` output from an
owned, pinned central Pod through `health.NewBackupSource`. Data streams to the
store without the metadata exec path's 64 KiB buffer. Capture requires both
operation Leases for the same installation and records the installation/Pod/
container identities, database UUID, full member inventory, leader, term and
committed/applied index. The sampled fence must remain unchanged across capture.
A concurrent transaction or election causes capture to fail and be retried by
the lifecycle Manager/controller; the provider never invents an index from the
backup file, which does not encode RAFT metadata.

The file backend publishes an immutable snapshot and record with file/directory
fsync and atomic directory rename. Its content-addressed reference binds the
record and the snapshot's SHA-256. Capture reopens the published files before
returning a reference. Verification after restart repeats that readback, native
record/JSON/schema-shape checks and the configured age/size policy. It rejects
foreign installation or database identities, a changed committed fence, altered
files and incomplete captures. Membership dispatch observes the database again
after backup readback to reject changes while storage was being verified.

`OpenFileStore` requires an existing operator-owned persistent directory, such
as a previously provisioned volume mount, with atomic rename and fsync support.
The caller must explicitly configure maximum age and size (at most 1 GiB).
Parsing also limits schemas to 4 MiB, JSON tokens to 1 MiB and each table to
250,000 rows. Published files are private to their owner. The stores expose a
restricted retention surface that lists only complete digest-keyed pairs and
deletes only pairs in the configured destination. File cleanup stays below the
`os.Root` boundary; S3 cleanup uses signed ListObjects and paired DELETE
requests. Pending, malformed, unknown and incomplete objects are skipped. File
digests prove integrity, not human approval or authenticity against an actor
who can rewrite both the store and trusted references.

The persistent lifecycle executor now records a verified NB/SB backup pair in
`status.lastBackup`, including the two immutable references, a pair ID, the
observed version, retained topology digest, destination and latest capture time.
When `spec.upgrade.backup.destination` is set, the built-in backend accepts a
local absolute `file://` URL that exactly matches the operator's `--backup-dir`,
or an `s3://bucket/prefix` URL signed with credentials already exposed through
the AWS credential environment. S3 publication uploads immutable digest-keyed
snapshot and manifest objects and returns a reference only after both succeed;
failed uploads are not deleted automatically. Before a version-only upgrade,
the controller re-verifies a matching pair or captures a new one, then
persists the durable upgrade checkpoint for the requested target version. The
checkpoint is consumed after the Chart write is accepted and cannot be reused
after a target, destination or topology change. Set
`spec.upgrade.backup.schedule` to a duration between one minute and thirty days
to request periodic verified NB/SB captures. A due scheduled capture runs under
both operation Leases, requires the retained topology and current observed
version, persists `status.lastBackup`, and returns before Chart rendering; it
does not change `status.applyCheckpoint` or authorize a Chart write. Scheduling
is skipped while a lifecycle operation or version-only upgrade is in flight,
and for HCP or data-plane-only installations. Set
`spec.upgrade.backup.retention.keepLast` and/or
`spec.upgrade.backup.retention.maxAge` to enable cleanup; leaving both empty
disables deletion. The controller runs cleanup under both operation Leases,
protects `status.lastBackup`, the verified upgrade checkpoint, active
lifecycle journal references and non-terminal `KubeOVNRecoveryRequest` plan
references, and records attempts in `status.backupRetention`. Cleanup runs
independently of the next scheduled capture deadline, and failed deletion
remains visible and is retried. S3 pair deletion is idempotent after a partial
request, so a transient failure between the two object deletes does not leave
an unremovable orphan.
When `spec.upgrade.pauseOnDegraded` is true, a version-only upgrade is paused
before backup capture or Chart rendering while the retained installation has a
`Degraded=True` condition. Clearing the condition allows the same target to be
re-evaluated. The subsequent rollout applies CRDs, then central resources, and
then data-plane DaemonSets only after central readiness is observed. This gate
does not implement rollback by itself; rollback uses the separate version-bound
checkpoint and restore executor. Non-destructive rejoin now reuses the explicit
member lifecycle journal. A version-only upgrade now
stores a separate durable `status.upgrade.checkpoint` with `Capturing`,
`Verified`, `Applying`, `Applied` and `Failed` phases. The checkpoint contains
both immutable database references and binds them to the source version, target
version, destination and retained topology. Only a verified pair can authorize
the Chart write; an in-flight or failed phase cannot be inferred from
`status.lastBackup`. Every restart and rollback admission also recomputes the
pair ID from both database references and rejects a checkpoint whose digest or
capture time is missing. The pair is coordinated durably, but the two native
`ovsdb-client backup` calls remain sequential and do not form an application
write barrier or an atomic RAFT snapshot. Cross-database rollback and
destructive restore use the verified pair and a lease-fenced native restore
transport; lost-quorum recovery still requires an explicit approval capability
and remains fail-closed. The
provider checks the native file structure and provenance, not all OVSDB schema
semantics; tests load the stored files into fresh OVSDB processes and verify
recovered data and row UUIDs for both OVN schemas. The live restore path also
rechecks cluster membership before and after each database and stops on a lost
Lease or topology change.
If restore fails after workloads were quiesced, the durable execution is marked
failed and the workloads remain quiesced for explicit operator recovery; the
controller does not automatically resume them against an uncertain database
state.

## Development

```bash
make test
make manifests
make chart-lint
make build
make e2e
```

The kind smoke test exercises plan status and the Kubernetes apply adapter.
It also sends actual probe traffic using kind's existing CNI, rejects a healthy
server that returns an incorrect Pod identity, and verifies recovery.
The release-chart integration tests render a checksum-pinned v1.16.13 archive
in all three installation modes and render a data-plane HCP `ssl:` installation
against an external TLS Secret. The HCP case verifies that offline rendering
removes the chart's live-lookup Secret template and rewrites every generated
Secret and RBAC reference to the admitted Secret name. These tests do not
install Kube-OVN or prove its network works. The CI job also starts isolated OVSDB processes to
check standalone/clustered metadata, leader failover and a minority follower
after quorum loss, plus real northd active/standby/pause and SB disconnect
behavior. Isolated ovs-vswitchd and ovn-controller processes verify `br-int`
with a dummy datapath and detect loss of SB connectivity.
The membership integration test uses real OVSDB processes and the persistent
backup and approval providers to check healthy leave, explicit replacement provisioning, approved kick,
quorum-loss refusal and preservation of committed data. This tests admin
operations and 3-to-2-to-3 convergence; it does not exercise Kubernetes node or
workload replacement.
Another live test prepares one new central member against actual OVN NB/SB
schemas. Both databases grow from three to four healthy members with preserved
row UUIDs/data, and preparation retry after joining retains the same identities.
This does not exercise the Kubernetes workload adapter or joint
replacement executor.
Set `OVSDB_TEST_BIN` to a directory containing `ovsdb-tool`,
`ovsdb-server`, `ovsdb-client` and `ovs-appctl` to run those tests locally. The kind job checks live Lease contention,
renewal past the initial expiry, release, and loss cancellation.
The CI `controller-deployment-handoff` job uses a three-control-plane kind
cluster and a synthetic host-network central fixture to drive the real default
controller through initial topology recording, explicit `replaceEndpoint`
replacement, and an approval-backed automatic replacement after a target Node
is cordoned and its Pod is deleted. It checks the pinned
Deployment/ReplicaSet/Pod identities, immutable lifecycle requests and
journals, approval completion, replacement StatefulSets, successor topology,
and a second StatefulSet retirement. The fixture runs native NB/SB processes;
this remains a focused lifecycle acceptance test rather than a full Kube-OVN
network installation.
The OVN runtime tests additionally use `OVN_TEST_BIN`, `OVN_TEST_SCHEMA_DIR`
and `OVS_TEST_SCHEMA`; see the CI workflow for the extracted package layout.

The manually dispatched `Kube-OVN installation` workflow provisions a separate
three-control-plane kind cluster without a default CNI. It installs the pinned
v1.16.13 chart through the operator, waits for workload and OVN runtime health, records live
NB/SB cluster status and probes Pod IP and Service DNS traffic between two nodes.
It uses the job's existing admin kubeconfig and creates the upstream chart's
ServiceAccounts and RBAC; it is separate from the default smoke workflow. The
repository RBAC now covers the operator's Pod exec, member preparation,
topology/retirement, approval status and common rendered chart resource paths.
The final permission set still needs review against the exact chart revision
and target cluster admission policies before production installation. Complete
Kube-OVN installation and transition verification remain separate acceptance
items. See [docs/design.md](docs/design.md) for the remaining execution
boundaries and safety contract.
