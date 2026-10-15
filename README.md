# Kube-OVN Operator

The Kube-OVN Operator reconciles a declarative `KubeOVNInstallation`. The
current implementation supports rendering checksum-pinned upstream Helm
charts, applying an initial resource set, and observing Kubernetes rollout.
The complete lifecycle requested in [Issue #7682](https://github.com/kubeovn/kube-ovn/issues/7682)
is still under development.

## Initial installation

The operator defaults to workload management. Set `spec.chart.source` to an
HTTPS `.tgz` URL and `spec.chart.digest` to the SHA-256 of the downloaded archive
(`sha256:<64 hex characters>`). The archive must contain the `kube-ovn` chart
with an `appVersion` matching `spec.version`. OCI sources are not supported yet.
HTTP is accepted only for literal loopback addresses in local integration tests.

The Helm SDK renders the original chart templates with the installation name
and namespace. It maps version, mode, external NB/SB endpoints, central storage
and the OVS update strategy to upstream values. Classic central placement uses
one label selector (default: `node-role.kubernetes.io/control-plane=`) and Node
InternalIPs. RAFT bootstrap requires exactly the requested number of addresses.
Those addresses are bootstrap input, never evidence of live RAFT membership.
The chart's remaining values use upstream defaults, including network CIDRs.

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
HCP, adoption and dynamic membership currently require those future executors.

`internal/apply` checks ownership with uncached reads, dry-runs the complete
batch, then creates new resources or uses server-side apply without force for
existing resources. It cannot adopt foreign resources. PVCs and resources
annotated `helm.sh/resource-policy: keep` receive installation labels but no
owner reference, so deletion of the installation does not garbage-collect them.
Cluster-scoped resources also use labels rather than an invalid namespaced owner.

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
External/HCP databases await a peer provider.

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
External/HCP databases still require a peer provider. Installation and RAFT
central operations acquire separate Leases with unique
operation identities. The membership/upgrade planners and OVN admin transport
are not yet a complete lifecycle execution state machine.

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
These checks stop subsequent writes. Strict fencing of requests already
submitted to an API/OVN server still requires transport support.

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
server. Joining still needs a workload provisioner and `ovsdb-tool join-cluster`.
A backup capture and verification adapter is available as described below.
Durable approval verification, the persistent NB/SB lifecycle state machine
and strict in-flight fencing remain required before wiring automatic membership
changes into the default reconciler.

## Database backups

`internal/backup.Provider` captures native `ovsdb-client backup` output from an
owned, pinned central Pod through `health.NewBackupSource`. Data streams to the
store without the metadata exec path's 64 KiB buffer. Capture requires both
operation Leases for the same installation and records the installation/Pod/
container identities, database UUID, full member inventory, leader, term and
committed/applied index. The sampled fence must remain unchanged across capture.
A concurrent transaction or election causes capture to fail and be retried by
the future lifecycle executor; the provider never invents an index from the
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
250,000 rows. Published files are private to their owner; the store never
deletes them. File digests prove integrity, not human approval or authenticity
against an actor who can rewrite both the store and trusted references.

This adapter is not yet wired to `spec.upgrade.backup.destination` or
`status.lastBackup`; the persistent lifecycle executor must own those updates.
S3, retention, backup scheduling, cross-database NB/SB checkpoints, recovery
execution and product RPO/RTO policy remain to be implemented. The provider
checks the native file structure and provenance, not all OVSDB schema semantics;
tests load the stored files into fresh OVSDB processes and verify recovered data
and row UUIDs for both OVN schemas. It does not restore over a live database or
authorize a lost-quorum recovery.

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
The release-chart integration test renders a checksum-pinned v1.16.13 archive
in all three installation modes. These tests do not install Kube-OVN or prove
its network works. The CI job also starts isolated OVSDB processes to
check standalone/clustered metadata, leader failover and a minority follower
after quorum loss, plus real northd active/standby/pause and SB disconnect
behavior. Isolated ovs-vswitchd and ovn-controller processes verify `br-int`
with a dummy datapath and detect loss of SB connectivity.
The membership integration test uses real OVSDB processes and the persistent
backup provider to check healthy leave, explicit replacement provisioning, approved kick,
quorum-loss refusal and preservation of committed data. This tests admin
operations and 3-to-2-to-3 convergence; it does not exercise Kubernetes node or
workload replacement.
Set `OVSDB_TEST_BIN` to a directory containing `ovsdb-tool`,
`ovsdb-server`, `ovsdb-client` and `ovs-appctl` to run those tests locally. The kind job checks live Lease contention,
renewal past the initial expiry, release, and loss cancellation.
The OVN runtime tests additionally use `OVN_TEST_BIN`, `OVN_TEST_SCHEMA_DIR`
and `OVS_TEST_SCHEMA`; see the CI workflow for the extracted package layout.

The manually dispatched `Kube-OVN installation` workflow provisions a separate
three-control-plane kind cluster without a default CNI. It installs the pinned
v1.16.13 chart through the operator, waits for workload and OVN runtime health, records live
NB/SB cluster status and probes Pod IP and Service DNS traffic between two nodes.
It uses the job's existing admin kubeconfig and creates the upstream chart's
ServiceAccounts and RBAC; it is separate from the default smoke workflow.
Production ServiceAccount permissions and complete Kube-OVN installation and
transition verification remain separate acceptance items. See [docs/design.md](docs/design.md) for the remaining
execution boundaries and safety contract.
