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
Northd, data-plane connections and network probes still require verification.
External/HCP databases await a peer provider. Observation requires pod list,
ReplicaSet/Deployment/StatefulSet read and `pods/exec` create permissions; the
limited shipped RBAC still requires environment-specific review and expansion.

`Ready` remains false until complete OVN and network verification is implemented. A
successful rollout reports `VerificationPending`; a valid plan alone reports
`PlanOnly`. Neither is evidence of network availability. Installation and RAFT
central operations acquire separate Leases with unique
operation identities. The pure
membership/upgrade planners and injected OVN admin interface are not yet a
complete lifecycle execution state machine.

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

## Development

```bash
make test
make manifests
make chart-lint
make build
make e2e
```

The kind smoke test exercises plan status and the Kubernetes apply adapter.
The release-chart integration test renders a checksum-pinned v1.16.13 archive
in all three installation modes. These tests do not yet install Kube-OVN or
verify a working network. The CI job also starts isolated OVSDB processes to
check standalone/clustered metadata, leader failover and a minority follower
after quorum loss. Set `OVSDB_TEST_BIN` to a directory containing `ovsdb-tool`,
`ovsdb-server` and `ovsdb-client` to run those tests locally. The kind job checks live Lease contention,
renewal past the initial expiry, release, and loss cancellation.

The manually dispatched `Kube-OVN installation` workflow provisions a separate
three-control-plane kind cluster without a default CNI. It installs the pinned
v1.16.13 chart through the operator, waits for workload rollout, records live
NB/SB cluster status and probes Pod IP and Service DNS traffic between two nodes.
It uses the job's existing admin kubeconfig and creates the upstream chart's
ServiceAccounts and RBAC; it is separate from the default smoke workflow.
Production ServiceAccount permissions and complete network/OVN verification
remain separate acceptance items. See [docs/design.md](docs/design.md) for the remaining
execution boundaries and safety contract.
