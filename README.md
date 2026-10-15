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

`Ready` remains false until OVN and network verification is implemented. A
successful rollout reports `VerificationPending`; a valid plan alone reports
`PlanOnly`. Neither is evidence of network availability. Lease resources exist,
but per-operation lease acquisition and fencing are still pending. The pure
membership/upgrade planners and injected OVN admin interface are not yet a
complete lifecycle execution state machine.

For plan inspection without fetching charts or changing workloads, run the
operator with `--plan-only`. This writes the plan ConfigMap and Lease resources
and keeps `Ready=False`.

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
verify a working network. See [docs/design.md](docs/design.md) for the remaining
execution boundaries and safety contract.
