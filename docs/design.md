# Implementation contract

This repository is the first implementation slice for
[kubeovn/kube-ovn#7682](https://github.com/kubeovn/kube-ovn/issues/7682).

`KubeOVNInstallation` is the single declarative input. The reconciler validates
the version contract, installation mode, central topology, backup policy and
HCP pairing before writing a plan. The plan ConfigMap is deliberately a stable
adapter boundary for the Kube-OVN Helm renderer and the OVN central admin
client. It contains no credentials and no shell commands.

Every installation has an installation Lease. RAFT installations also have a
separate central-membership Lease. The membership actions are ordered as
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
OVNDB transport.

The status phase distinguishes installing, adopting, upgrading, recovering
and degraded plans. `Ready` means the plan passed safety validation and was
serialized; the workload adapter must add the observed central leader,
readiness and network probe evidence before reporting data-plane availability.
The `KubeOVNRecoveryRequest` API records an auditable human decision and does
not perform a database wipe automatically.

## Kubernetes workload apply contract

`internal/apply.Client` is a dynamic-client boundary for rendered workload
objects. Every object must have a fixed name and a namespace matching the
installation. The adapter stamps these labels and, for namespaced objects, a
single controller owner reference:

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
`WorkloadFactory`; leaving both workload fields unset preserves the plan-only
default.

## Next adapter boundaries

1. Connect the chart renderer to a Helm SDK or sidecar that returns typed
   workload objects and uses the apply contract above.
2. Supply an OVNDB transport (libovsdb or an authenticated admin sidecar) that
   implements the `internal/ovnadmin.Client` interface and executes one
   membership operation under the central Lease.
3. Add backup and network probe providers; a missing provider keeps the phase
   `Degraded` when the corresponding gate is required.
4. Add HCP peer transport using Secret references only. Private key material
   must never be copied into a CR, status field or ConfigMap.
