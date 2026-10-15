# Joint NB/SB membership execution

`internal/lifecycle.Executor` performs one durable addition, removal or replacement
of a central member. `NewKubernetes` connects it to the actual owned-Pod admin
transport, backup capture/verification, permanent-loss approvals and Kubernetes
member provisioner. The caller holds both installation-bound Leases and supplies
an uncached client and an existing trusted backup store with age/size limits.

The default installation reconciler does not yet select operations or invoke this
executor. Initial-apply checkpoint restrictions remain in force. This executor
is part of the complete lifecycle implementation, not automatic node replacement
or a complete installation/upgrade controller.

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
external storage, pinning its UID and complete admitted spec. Retirement uses a
resourceVersion-checked replica update; any unrelated spec or identity change
blocks it. It retains the zero-replica StatefulSet, PVCs, PVs and identity records.
It does not remove a Namespace, database or persistent volume.

An ordinary multi-replica Chart Deployment cannot guarantee which member a
replica decrease removes. Migrating those workloads to independently managed
members remains a prerequisite for their retirement through this executor.
Runtime image publication/selection, automatic storage and TLS Secret lifecycle,
node placement/changes, lifecycle-authorized inventory transitions and generation transitions,
installation apply-checkpoint authorization and post-transition network checks
remain part of controller integration.

Backup pairs are conservative sampled fences, not atomic cross-database snapshots
or an application write barrier. Continuous writes can prevent a matching pair
from being authorized. Joint observations and Kubernetes writes are not an atomic
transaction, and Lease cancellation cannot retract already accepted API or OVN
commands. Transport-level fencing is still required for strict in-flight isolation.

Unit tests cover partial NB/SB catch-up, lost startup/removal responses, journal
write failures, late peer failure at dispatch, identity/generation drift, approvals,
operation exclusion, completion receipt recovery and retirement guards. The kind
member smoke test runs native preparation, real streamed backups, joint replacement
and old StatefulSet retirement with a reconstructed executor at each step. It uses
one kind node and a one-member seed per database; it does not establish multi-node
HA replacement or full Kube-OVN network acceptance.
