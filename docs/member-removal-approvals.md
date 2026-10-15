# Permanent-loss approval contract

An unhealthy Pod is not evidence of permanent data loss. Before `cluster/kick`,
an authorized human creates a namespaced `KubeOVNMemberRemovalApproval` that
identifies the installation name and UID, one operation UUID, one or both OVN
databases, each database's full cluster and lost server UUID, its exact canonical
literal-IP RAFT address, a reason and an expiry. Each database may appear once.
The lifecycle executor must pin the returned approval name and UID; a record
recreated with the same name cannot replace it.

## Deployment prerequisites

Permission to create this resource is permission to approve permanent loss.
There is no self-reported approver field that can grant that authority. Retain
Kubernetes API audit events for create, update and status requests so the
authenticated principal, resource UID, decision and changes can be investigated.
The `approvedBy` string on `KubeOVNRecoveryRequest` is user input and is not used
by this provider. Approval of a kick does not authorize restoring a database,
rebootstrapping without quorum, deleting storage or changing credentials.

`config/rbac/member-removal-approval-optional.yaml` contains two namespace-scoped
roles, without bindings:

| Principal | Resource permissions |
| --- | --- |
| Human approver | Create/read approvals and update to revoke them |
| Operator recorder | Read approval spec and update/patch approval status |

Review the installation namespace and bind these roles to separate existing
principals as part of deployment authorization. The file is excluded from the
default Kustomize and Helm installation. The default operator account cannot
create, update or delete approval records; the adapter is not wired into the
default reconciler yet. No role grants approval deletion. The recording role
must not be combined with approval creation privileges on the operator account.

Keep records without owner references. The provider rejects records subject to
owner garbage collection or pending deletion. Namespace administrators and
cluster administrators can still remove resources, and metadata update privileges
are not an immutable audit store. Retained API audit logs are the independent
history; this CRD does not protect history against cluster administrators or
actors who can change its admission/RBAC configuration.

## Reservation and completion

1. Acquire the installation Lease and then its central-membership Lease using
   the installation UID. Persist the lifecycle operation UUID independently.
2. With an uncached client, `Reserve` validates the decision and records its
   operation UUID, SHA-256 decision digest and reservation time. The same
   operation can retry after restart without rewriting this reservation.
3. Capture and verify a matching backup. `VerifyPermanentLoss` additionally
   requires the exact approved database/member/address, a healthy quorum and
   leader, an unhealthy non-leader target, and an active reserved approval.
4. The admin transport repeats database observation after backup readback and
   re-reads the approval before dispatch. Its context expires no later than the
   approval. Revocation, expiry, identity replacement or completed status blocks
   a new request. An already accepted OVSDB request may still finish; a timeout
   has an uncertain result, so always observe before retrying.
5. Reobserve every approved database under the Leases. Only after each has
   healthy quorum and no longer lists its old server UUID may `Complete` record
   the cluster ID, term, committed index and observation time. The lifecycle
   caller is responsible for supplying fresh observations from the live admin
   transport; arbitrary snapshots are not independent evidence of live state.
   Completion after revocation/expiry records an already converged action and
   does not authorize another one.

The API enforces an immutable decision, one-way revocation, a stable reservation,
the Reserved-to-Completed transition and immutable completed audit status.
The provider also validates these boundaries on read and uses resourceVersion
checks for writes. A conflict is returned for fresh observation and retry;
status writes do not overwrite a concurrent decision or revocation.

This is an approval adapter. It does not provision a replacement, coordinate
NB/SB mutations, change Kubernetes replicas or implement the persistent node
lifecycle executor. An approval containing both NB and SB cannot be completed
after only one database has converged.

## Validation scope

Unit tests cover identity and decision changes, invalid observations, revocation,
expiry, restart/idempotency, conflicting writes and complete database coverage.
The real OVSDB integration uses this provider with a fake Kubernetes store to
reserve, verify, kick and complete a lost-member action. The separate kind
approval smoke test uses a real API server to check CEL admission, status
conflicts, irreversible revocation, completed audit immutability and denial of
approval creation/update/deletion to the default operator account. It uses the
job's existing admin client and creates no credentials or RBAC bindings.

The kind test's database observations are synthetic, and the isolated OVSDB
test does not replace Kubernetes node-lifecycle or full network acceptance.
