# Offline lost-quorum reconstruction contract

The operator now has an in-process contract for the inputs required by a future
offline OVNDB reconstruction executor. It is implemented by
`internal/recovery.Plan` and is deliberately separate from the healthy-quorum
restore path.

A valid plan binds all of the following:

- the installation, operation, source version and target version;
- a human-created recovery approval name, UID and immutable decision digest;
- the content-addressed NB/SB backup pair and both source cluster UUIDs;
- two new target cluster UUIDs, one for each database;
- the complete retained member storage/process identities; and
- every target member's fresh storage identities and explicit NB/SB RAFT and
  client addresses.

Canonicalization sorts members and databases, clones caller-owned slices, and
returns a SHA-256 plan digest. Validation rejects missing approval evidence,
malformed or reused cluster IDs, duplicate endpoints, even target member
counts, reused workload/storage identities, and backup pair IDs that do not
match the two database references.

Plan validation has no side effects. It does not create or replace PVC
contents, stop old OVN processes, invoke `ovsdb-tool`, start a new RAFT cluster,
stream a backup, or mutate Kubernetes objects. The current controller continues
to use the healthy-leader fenced restore path. A later integration must consume this
contract through a dedicated privileged workload that proves old-process
isolation and new storage ownership before creating the target databases.

## Native execution boundary

`internal/recovery.Executor` is the next, deliberately narrow execution layer.
It accepts one target member at a time and requires three capabilities before
writing anything:

1. an authenticated `backup.Store` whose records match the installation,
   source cluster IDs, and content digests in the plan;
2. an `Isolation` implementation that proves every retained process is
   isolated and that the new root belongs to the selected target member; and
3. a `Native` implementation that creates and inspects both clustered database
   files with the exact target cluster UUID, server UUID, and RAFT address.

The executor takes an exclusive lock on a private, already provisioned target
root, stages and authenticates both backups before creating either native
database, and invokes the native capability only below the operation directory.
It rechecks isolation and target ownership before each database and before
publishing or returning a completion receipt. The provider must keep isolation
enforced throughout execution; repeated observations alone cannot guarantee
that a stopped process stays stopped. It returns the observed native
identities for durable persistence by the caller. It never starts
`ovsdb-server` or `ovn-northd`, removes retained storage, changes a PVC, or
mutates Kubernetes.

The executor now requires an `IsolationSession` in addition to the point-in-
time `Isolation` checks. `Begin` must establish fencing and target ownership
and return an `IsolationGuard`; the guard is checked before every native write
and before the completion receipt, then closed before success is returned. A
provider that only implements independent observations is rejected before any
backup is staged. This seam is intentionally injectable so a future provider
can hold the fence-agent Lease token and verify the target PVC identity for the
whole operation.

`recovery.FencedIsolationSession` is the fail-closed provider skeleton for
that seam. It requires three injected capabilities: a central Lease token
source, a `FenceController` that isolates/checks/releases every retained
member through an atomic server-side fence, and a live
`StorageOwnershipVerifier` for the target root. It refreshes and compares the
Lease token before each retained-member action, rolls back partial fencing on
begin failure, checks all retained members and target storage for every guard
check, and never releases fencing with a token that has been taken over. The
repository does not provide a default controller or PVC verifier; readiness,
`/proc` scans, and marker files cannot satisfy these interfaces.

`internal/recovery.KubernetesStorageOwnershipVerifier` is the first live
ownership adapter. It resolves the target Pod and PVC by immutable UID, checks
the controller workload UID, node UID, `ovn-central` container ID, namespace
and PVC mount, and requires the PVC to remain Bound while the target root is an
existing directory. It does not infer ownership from names or marker files. A
real retained-member fence controller is still required before the executor
can be enabled by the controller.

The fence-agent now exposes `/v1/fence/isolate`, `/v1/fence/check` and
`/v1/fence/release` over the same mTLS channel. It requires the current Lease
token at the HTTP gate, shares the member PID namespace, and suspends the
`ovsdb-server` and `ovn-northd` processes named by private runtime PID files.
Each check re-reads the PID files and `/proc` command line/state before native
writes. The sidecar remains fail-closed when the Lease expires; it does not
resume a process from a stale token. `RemoteFenceController` adapts this
protocol, but controller wiring and a live endpoint resolver are still pending.

The operation directory is journaled before the first native write. A retry
revalidates the exact canonical plan and target member, reuses only private
single-link backup files whose bytes still match the authenticated record,
reinspects any existing database identities, and publishes `result.json`
atomically after both databases pass. An interrupted operation can therefore
resume the missing database without recreating a completed one; unknown files,
changed identities, and malformed journals stop the retry.

The upstream `ovsdb-tool create-cluster` command generates a new cluster UUID
and has no option to select one. A stock tool therefore cannot implement the
`Native` capability safely: the executor rejects an observed UUID that differs
from the approved target. A privileged workload must provide a native adapter
with this exact identity binding (or an equivalent audited native operation)
before this layer can be used in production. This is intentional; random
cluster creation must not be reported as completion of lost-quorum recovery.

`internal/recovery.CommandNative` defines the adapter boundary for that helper:
it invokes `--target-cluster-id=<uuid> create-cluster <db> <contents> <local>`
and then invokes `inspect <db>`. The inspection command must return one JSON
object containing `clusterID`, `serverID`, `address`, and `clustered`; reading
the identity in one helper invocation avoids accepting a file that changed
between separate metadata queries. Output is bounded and every field is
validated. The helper path must be an absolute, non-writable executable
without setuid, setgid, or file capabilities. The repository ships this
adapter as `/recovery-native`; a stock `ovsdb-tool` is expected to reject the
extra identity flag or fail the subsequent identity check.

The adapter translates the recovery protocol to
`ovsdb-tool --cid=<uuid> create-cluster` and reads one `db-raft-header` result;
it rejects joining headers and verifies that the created cluster ID equals the
approved target. The OVS binary must contain the Kube-OVN target-CID
`create-cluster` extension. The stock binary accepts the global `--cid` option
but still generates a random ID, so `/recovery-native` detects and rejects
that result. The helper is not yet wired into Kubernetes storage replacement
or process startup.

## Dedicated rebuild workload

`recovery.WorkloadSpec` renders a one-shot, digest-pinned Kubernetes `Job` for
the future rebuild executor. The Job is separate from the ordinary operator
Deployment and central member workloads. It is pinned to the target node, uses
`hostPID` and `hostNetwork`, runs the versioned `/recovery-native` executable as
a privileged container, and has a zero retry budget with a bounded deadline.
Its root filesystem and every input volume are read-only except for the
selected target PVC.

The builder describes a target PVC mounted read-write, retained member PVCs
and the backup PVC mounted read-only, immutable plan and isolation metadata
ConfigMaps, and a read-only host `/proc` mount. These mounts are not proof of
isolation: a target-node process scan cannot establish that old processes on
other nodes are fenced, and a JSON ownership marker cannot establish live PVC
ownership.

The `--workload-plan` entry point therefore validates the supplied plan and
isolation audit metadata, then fails closed with an unavailable-provider error
before opening the target root or backup store. It does not invoke the native
adapter or write a completion receipt. A real provider must enforce isolation
for every retained process and verify live target storage ownership before this
entry point can be enabled. The controller does not create these rebuild Jobs.
The standalone native create/inspect adapter remains available to callers that
provide the required execution boundary.

`IsolationProof` is audit metadata, not an isolation capability. It binds the
canonical plan digest, operation ID and complete retained member set. Validation
accepts timestamps at most five minutes old, with at most one minute of future
clock skew, and rejects missing, expired, partial or mismatched metadata.
Successful metadata validation never authorizes writes on its own.

Current CI exercises the injected executor capabilities and the command's
refusal to execute without an enforced provider. It does not demonstrate real
lost-quorum reconstruction, member startup or data-plane recovery through this
workload path.
