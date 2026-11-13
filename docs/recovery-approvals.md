# Destructive recovery approvals

`KubeOVNRecoveryApproval` is a human-created, namespace-scoped authorization for
a destructive recovery operation. It is separate from
`KubeOVNRecoveryRequest`: a request describes the desired operation, while the
approval records the independent decision to permit a restore when the normal
OVN quorum evidence is unavailable.

## What an approval binds

The decision fields in `spec` are immutable after creation; only `revoked` can
change. The decision must bind all of the following to the
installation that will be changed:

- `installationRef.name` and `installationRef.uid`;
- `installationGeneration`;
- `mode`, `sourceVersion`, and `targetVersion`;
- the verified backup pair digest in `backupRef`;
- the retained topology digest in `topologyDigest`;
- the exact target Chart digest in `chartDigest`;
- a non-empty `reason` and a future `expiresAt`.

For a `KubeOVNRecoveryRequest` operation, `requestRef` additionally pins the
request name, UID, and generation. A version rollback leaves `requestRef` empty
and binds directly to the installation generation and version pair. The
`requestedBy` field on a recovery request is audit input only; it never grants
authority.

The `revoked` field can be changed from `false` to `true` once. Revocation
cannot be reversed. An expired or revoked approval cannot start a new operation.

The executor revalidates admission when entering `Restoring`, after the durable
`Quiescing` phase. A reservation does not bypass expiry, revocation, approval
UID or binding checks after a controller restart. Without an approval, live
quorum must still be healthy at this boundary. Failed revalidation invokes no
database restore and leaves the workloads quiesced for explicit intervention.
This boundary does not cancel an already submitted restore or provide atomic
revocation across the sequential NB/SB writes.

## Create a request-bound approval while the controller is running

A destructive request can be prepared with `spec.suspend: true`. It remains
`Pending` with `Accepted=False`, reason `Suspended`, and has no admitted plan or
execution. It still expires. The normal installation reconciler continues to
run, and the request cannot acquire recovery authority while suspended.

1. Create the suspended request and read its API-assigned UID and generation
   `N`, the installation generation, and the verified backup/topology/Chart
   digests.
2. Create a `KubeOVNRecoveryApproval` binding that request UID and generation
   `N+1`. This is the generation of the single spec update that will both attach
   the approval and resume the request. Pin all other decision fields normally,
   then read the approval's API-assigned UID.
3. In one request update, set `approvalRef` to that exact approval name/UID and
   set `suspend: false`. Include the current request `metadata.resourceVersion`
   as a precondition. Status-only conflicts can be retried after rereading;
   a changed request UID or spec generation requires a new approval decision.
4. Confirm that the resulting API generation matches the approval binding.
   The controller now records the immutable plan and reserves the approval
   before restoring.

Suspension cannot be changed after a plan or execution has been recorded. It
is a preparation gate; cancellation of an admitted operation still follows the
existing fail-closed recovery contract. Requests that omit `suspend` keep their
existing immediate admission behavior. `requestedBy` remains audit input only.

## Lifecycle

The caller must store the approval name and UID in the admitted rollback or
recovery plan. Before any database write, the operator:

1. rereads the approval and validates every binding and its expiry;
2. reserves it by writing `status.phase=Reserved` and a deterministic
   `status.decisionDigest`;
3. executes the already admitted, fenced operation;
4. writes `status.phase=Completed` only after both databases and the target
   workloads converge.

The API enforces separate reservation and completion writes: completion cannot
skip reservation, and an existing reservation cannot be cleared. Its decision
digest and reservation timestamp are immutable. Only `Completed` may carry a
completion timestamp, which cannot precede reservation; the reservation must
precede expiry. Completed audit evidence cannot be rewritten or reset.

The reservation is retryable for the same immutable decision. A completed
approval is one-time use and cannot authorize another operation. A lost Lease,
changed identity, changed topology, changed backup or changed Chart digest
stops the operation and leaves the approval unusable for that different target.

Revocation leaves the reserved decision digest unchanged. It prevents further
admission, including after a restart, but permits recording completion after
an already admitted operation has converged. Completion validates the full
reservation identity and timestamp even when retried after restart or expiry.
It never grants permission to perform another database write.

`rejoin` does not accept a destructive recovery approval. `reconvert`,
`freshBootstrap`, and version rollback may use one when quorum cannot be
proven. The normal operator account has only read access to the approval spec
and status update access; it cannot create or change the decision. Human
approval roles are supplied as optional, unbound manifests in
`config/rbac/recovery-approval-optional.yaml`.

## Example

The values below are illustrative and must be replaced with values read from
the target installation and its verified backup status:

```yaml
apiVersion: operator.kubeovn.io/v1alpha1
kind: KubeOVNRecoveryApproval
metadata:
  name: rollback-v1-16
  namespace: kube-ovn-system
spec:
  installationRef:
    name: prod
    uid: 00000000-0000-0000-0000-000000000000
  installationGeneration: 42
  mode: rollback
  sourceVersion: v1.17.0
  targetVersion: v1.16.0
  backupRef: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  topologyDigest: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  chartDigest: sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  reason: quorum is unavailable and the verified backup pair is required
  expiresAt: "2026-12-31T00:00:00Z"
```

The approval object does not contain credentials, private keys, database files,
or shell commands. The repository ships the operator-side fenced transport and
an optional mTLS `fence-agent` deployment. Disposable three-node Kind
acceptance covers Lease takeover/expiry and native NB/SB restore with healthy
quorum; see [transport fencing](transport-fencing.md). Production deployment
and acceptance in the target failure environment are still required. Until a live
fence endpoint passes readiness, the operator remains fail-closed even when an
approval exists.

The provider and binding rules are covered by unit tests. Disposable Kind API
acceptance covers immutable decisions, one-way revocation, reservation and
completion audit, provider restart, stale resource-version conflicts, and
one-time consumption in `rollback`, `reconvert`, and `freshBootstrap` modes.
That API/provider fixture uses synthetic backup/topology digests and real
request UIDs; it does not restore databases. A real lost-quorum
restore still requires the configured fenced executor and a multi-node
acceptance test of the complete controller recovery/approval workflow; the
healthy-quorum agent transport test does not establish that capability.


## Healthy-quorum controller acceptance

The disposable three-node controller handoff fixture also submits a real
`KubeOVNRecoveryRequest` in `reconvert` mode. It enables scheduled capture long
enough to obtain a fresh native NB/SB pair, disables the schedule, and checks
that replaying the completed member journal does not replace that pair. It
changes a marker in both databases and requires a foreign backup reference to
fail admission without changing data or rollout state.

For the valid request, acceptance observes the immutable plan and durable
`Restoring`, `Workloads`, `Verifying`, and `Succeeded` phases. Before restore,
all three owned workload controllers must carry the plan-bound quiesce marker
and saved rollout state. Completion must remove those markers, restore the
original Deployment pause/StatefulSet strategy, retain native cluster/server
identities, and expose the restored snapshot marker on every node for both
databases. Acceptance repeats the same native restore after rediscovering the
current processes, and each successful request must be followed by an
installation Ready condition for the current generation. Only the exact
verified spec, manifest and version become the recovery apply checkpoint;
unrelated later inputs still require their own lifecycle authorization. The
API request and controller status are used directly; no backup, plan or
execution status is fabricated by the test.

Smoke fixtures wait for their operator process to exit before handing the
cluster to subsequent Lease/approval checks.

Acceptance pins create/list/watch to one existing API server in the HA Kind
fixture and primes its CRD watch cache with a cached list before creating a
recovery request, then watches from that create response's resource version.
The existing kubeconfig still supplies the CA and client authentication.
A watch error, including expired history, remains a failure; the fixture never
substitutes a later status for missing durable phase evidence.

The request includes a fractional-second expiration. The persisted plan's
timestamp uses the API status field's second precision, while its immutable
digest and expiration admission retain the request's full precision.

Temporary contention for either operation Lease leaves the request executing
at its durable phase and retries admission. It does not invoke a restore step
while another holder owns the Lease. Lease loss and restore failures remain
terminal failures.

An executing request whose plan pins the current installation UID owns its
durable recovery phases. Normal installation reconciliation yields to it and
rechecks that ownership under the installation Lease before mutations, so
routine Lease events cannot starve recovery or undo rollout quiescence.

The first restore uses healthy quorum admission. The second creates a suspended
public request, creates a separate approval bound to its real UID and final
API generation, and atomically resumes it with the approval's real UID. While
suspended, it must leave data and rollout state unchanged. The real controller
must record `Reserved` before restore, reject a late suspension change through
API validation, and record immutable `Completed` audit after convergence.
The fixture never writes request execution or approval status itself.

Before restoring either database, the lifecycle manager authenticates and
fully stages both native snapshots against the retained cluster IDs, server
IDs and RAFT addresses. A corrupt, missing, expired or foreign SB backup
therefore fails before NB receives restore input. Staging uses private,
unlinked files and bounded streaming; both handles are closed on failure or
completion. It requires temporary disk space for both snapshots (up to 2 GiB
with the maximum policy), rather than buffering them in operator memory.
Execution consumes those exact bytes without reopening the store, and still
rechecks live topology, age, the operation Lease and remote fencing.

The fixture finishes by corrupting only its disposable SB backup, submitting
a new public recovery request and requiring a staging failure before any
NB/SB marker changes. Membership must remain unchanged on all three nodes,
and failed recovery must leave rollout quiesced. This deliberate failure is
separate from the two successful recoveries and their Ready checks.

`backup.Provider.Stage` can verify a stored capture without a live database
source, while still requiring the installation's central Lease. This is an
input boundary for future offline reconstruction; it grants no authority to
isolate old processes, replace storage, create a new RAFT cluster or bypass
the current healthy-quorum restore transport. Later NB/SB execution remains
sequential: a runtime failure after the first write is not atomic rollback.

This uses healthy quorum and verifies the public approval consumption path. It
does not establish lost-quorum reconstruction or approval consumption during
that failure, a version-changing rollback, or an application write barrier
across the sequential NB/SB captures.


Member-command process identity is checked independently of Pod readiness:
leaving NB can make the shared container unready before its separately
verified SB leave. Ownership, Pod UID, node, container ID, restart count and a
running central container remain pinned. Native status still proves database
identities and quorum, mutations still require the current remote fence, and
complete healthy topology discovery still requires every Pod to be Ready.
