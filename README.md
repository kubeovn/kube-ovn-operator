# Kube-OVN Operator

The Kube-OVN Operator manages a declarative `KubeOVNInstallation` and turns a
versioned installation contract into an observable, serialized reconciliation
plan. It is designed for full, control-plane-only, data-plane-only and hosted
control-plane (HCP) topologies.

The first release deliberately makes the safety boundary explicit: the
operator never infers live OVNDB membership from `NODE_IPS`, never wipes a
database after a snapshot, and never changes a central replica count until a
membership plan has passed its quorum and backup gates. A rendered plan is
stored in a ConfigMap and guarded by an installation Lease and a dedicated
central-membership Lease. Chart rendering and the central admin adapter can be
added behind these stable boundaries without changing the API.

## Quick start

```bash
make test
make manifests
make docker-build IMG=ghcr.io/kubeovn/kube-ovn-operator:dev
kubectl apply -f config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml
kubectl apply -f config/samples/operator_v1alpha1_kubeovninstallation.yaml
```

The sample produces a plan ConfigMap and status Conditions. A production
deployment should set a chart source and digest, backup destination, and HCP
TLS references before enabling workload management.

## API safety rules

* `mode` is `full`, `controlPlaneOnly`, or `dataPlaneOnly`.
* RAFT central replicas are odd and limited to 3 or 5; single mode is exactly
  one replica and cannot be changed in place to RAFT.
* `dataPlaneOnly` requires both external NB and SB endpoints.
* Version changes enter an observable upgrade plan. A required backup and a
  healthy quorum are gates for central membership changes.
* Loss of quorum, ownership conflicts, incompatible versions, and missing HCP
  endpoints pause reconciliation in `Degraded` or `Recovering`.

See [Issue #7682](https://github.com/kubeovn/kube-ovn/issues/7682) for the
feature request and [docs/design.md](docs/design.md) for the implementation
contract.
