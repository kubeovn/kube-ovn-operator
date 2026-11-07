# Joint NB/SB membership execution

`internal/lifecycle.Executor` performs one durable addition, removal or replacement
of a central member. `NewKubernetes` connects it to the actual owned-Pod admin
transport, backup capture/verification, permanent-loss approvals and Kubernetes
member provisioner. The caller holds both installation-bound Leases and supplies
an uncached client and an existing trusted backup store with age/size limits.

The default installation reconciler now discovers and resumes the durable
operation journal before Chart rendering. It holds both operation Leases, checks
the status-pinned original topology reference and immutable completion receipt,
and fences ordinary Apply while the operation is incomplete. After completion,
the controller performs a fresh final discovery and writes a new immutable
topology baseline while retaining the old record. It admits one addition at a
time when an unused `spec.central.memberRequests` entry pins the node, bound
PVC/PV, digest-pinned image and NB/SB listener addresses. The original Chart
Deployment remains at its retained count while the new member is prepared and
started as an independently managed StatefulSet. Replica reductions require
one exact retirement candidate. An independently managed, owned one-member
`OnDelete` StatefulSet can be selected automatically; an ordinary multi-replica
Chart Deployment requires a request with `retireEndpoint`, which pins one
exact Pod, its ReplicaSet and every surviving Pod before a one-step scale-in.
A request may set `replaceEndpoint` to combine the new member with removal of
one exact retained StatefulSet or Chart Deployment Pod at the same replica
count. The replacement pins the old workload UID, ReplicaSet UID when
applicable and admitted spec before preparation and reuses the same joint
NB/SB catch-up, backup, quorum and retirement gates.

If the retained Chart Deployment Pod has already disappeared because its Node
was deleted, replaced, cordoned or became NotReady, or has been replaced by a
different Pod UID, the request must carry a pre-created
`KubeOVNMemberRemovalApproval` reference in
`central.memberRequests[].removalApprovalRef`. The request may omit
`replaceEndpoint`; the selector then chooses exactly one proven lost endpoint
and rejects ambiguity. Admission keeps the old Pod UID from the retained
topology, fixes every surviving Pod UID and allows at most one healthy
replacement Pod from the pinned ReplicaSet. The executor uses the approval only
if the old database member is unhealthy, then removes the unplanned replacement
Pod by a guarded one-step Deployment scale-down after both NB/SB configurations
converge. The selector reuses the approval decision's `operationID` for the
durable lifecycle request, allowing the approval to be created before the
replacement request. A missing endpoint on a stable Ready Node is rejected
rather than treated as permanent loss.

## Durable contract

`Begin(ctx, Request)` admits an immutable request containing installation UID and
generation, operation UUID, complete original NB/SB inventories, optional new
member preparation/storage/runtime input, and the exact old workload to retire.
Each peer pins its full database server UUID, RAFT address, client address and
transport endpoint. Both inventories describe the same central workloads.
Preparation remote addresses must match the original complete inventory.

A retained immutable ConfigMap stores the request. One retained mutable
installation journal serializes operations with resourceVersion comparisons.
Its reference pins both the journal UID and the immutable request UID. Reusing
an old reference cannot drive a successor operation. Request, journal and
completion records have no owner reference so installation deletion cannot
garbage-collect the history needed to identify retained database files.

`Step(ctx, Reference)` performs one resumable phase or dispatch attempt:

1. `Preparing` calls the member provisioner and records its immutable result and
   record UID. Preparation alone never starts an OVN server.
2. `Starting` captures and verifies both backups against fresh database evidence,
   then starts the exact prepared identities. This phase is persisted before
   startup, so an accepted create with a lost response can be retried safely.
3. `CatchingUp` observes the complete original-plus-new inventory. Both databases
   must include the new member with applied committed data and healthy retained
   members before either old member may leave.
4. `Removing` persists each database's backup reference and exact removal intent
   before dispatch. Healthy targets leave; unhealthy targets require a reserved,
   pinned permanent-loss approval and a verified kick. The other database is
   checked again at native dispatch, after target backup/approval verification.
5. `Retiring` requires both configurations to exclude their old identities and
   have healthy retained members. It records any approval completion, scales only
   the pinned old one-member StatefulSet to zero, and waits for its Pods to disappear.
6. `Complete` retains an immutable first completion receipt. A lost final journal
   update recovers that receipt without rewriting its observed terms or indexes.

An asynchronous unixctl acknowledgement does not advance the operation past
removal. After an uncertain response, the next step reobserves both databases.
An old member's absence is accepted only after its removal intent was persisted.
Unexpected members, missing retained members, changed identities, loss of quorum,
lagging replacements, changed backup fences or changed installation generation
stop progression. Errors retain the operation and its storage for investigation
or an exact retry; no timeout initiates a bootstrap or data reset.

The inventory includes temporary members even when its size differs from
`spec.central.replicas`. Thus an orchestrator can compose intermediate additions
or removals and same-size replacement without pretending that a temporary
four-member configuration is the final desired replica count.

## Retirement and remaining integration

`PinRetirement` accepts only an owned one-member `OnDelete` StatefulSet with
external storage, pinning its UID and complete admitted spec. Deployment
retirement uses `PinDeploymentRetirement` to pin the Deployment UID, one stable
ReplicaSet UID, the target Pod UID and all surviving Pod UIDs. It sets the
target's `controller.kubernetes.io/pod-deletion-cost` to the minimum value,
then performs one resourceVersion-checked scale-down after joint membership
convergence. The executor waits for the target Pod to disappear and blocks if
any survivor is replaced or lost. StatefulSet retirement retains the
zero-replica StatefulSet; both paths retain storage and identity records. They
do not remove a Namespace, database or persistent volume.

Replica count alone never identifies a safe Deployment member. The selector
requires one and only one implicit StatefulSet candidate, or an explicit
`retireEndpoint` for a Chart Deployment. It does not infer a target from
replica ordering or pod names. A desired increase
requires a complete explicit member request, including node, bound PVC/PV,
runtime image and listener addresses, and is therefore not synthesized from
Chart values. Member requests are additions to an installation with a retained
topology; the initial Chart install must establish that baseline first. The
provisioner creates the native preparation result during the durable `Preparing` phase; callers do not inject generated server IDs.
Runtime image publication/selection, automatic storage and node placement/changes,
broader automatic Chart workload migration and post-transition network checks
remain part of controller integration. The selected one-step reduction path
authorizes and records its next apply checkpoint after the immutable completion
receipt and successor topology are pinned.

Backup pairs are conservative sampled fences, not atomic cross-database snapshots
or an application write barrier. Continuous writes can prevent a matching pair
from being authorized. Joint observations and Kubernetes writes are not an atomic
transaction. The OVN mutation path now requires the explicit
`ovnadmin.FencedRunner` capability and passes a `lease.FenceToken`; a plain
`pods/exec` runner fails closed. The remote executor must atomically validate
that token to fence a request already in flight.

Unit tests cover partial NB/SB catch-up, lost startup/removal responses, journal
write failures, late peer failure at dispatch, identity/generation drift, approvals,
operation exclusion, completion receipt recovery and retirement guards. The kind
member smoke test runs native preparation, real streamed backups, joint replacement
and old StatefulSet retirement with a reconstructed executor at each step. It uses
one kind node and a one-member seed per database; it does not establish multi-node
HA replacement or full Kube-OVN network acceptance.
