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

This package has no side effects. It does not create or replace PVC contents,
stop old OVN processes, invoke `ovsdb-tool`, start a new RAFT cluster, stream a
backup, or mutate Kubernetes objects. The current controller continues to use
the healthy-leader fenced restore path. A later integration must consume this
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
root, copies each verified backup into that root, and invokes the native
capability only below the operation directory. It returns the observed native
identities for durable persistence by the caller. It never starts
`ovsdb-server` or `ovn-northd`, removes retained storage, changes a PVC, or
mutates Kubernetes.

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

The native adapter runs in a one-shot, digest-pinned Kubernetes `Job` rendered
by `recovery.WorkloadSpec`. The Job is separate from the ordinary operator
Deployment and central member workloads. It is pinned to the target node, uses
`hostPID` and `hostNetwork`, runs the versioned `/recovery-native` executable as
a privileged container, and has a zero retry budget with a bounded deadline.
Its root filesystem and every input volume are read-only except for the
selected target PVC.

The Job mounts the target PVC read-write, every retained member PVC read-only,
the operator-owned backup PVC read-only, an immutable plan ConfigMap, and an
independently produced isolation-proof ConfigMap. It also mounts the host
`/proc` tree read-only so the helper can reject a process that still references
the rebuild's retained mount paths. The helper refuses to start unless the
target directory is private and owned by its effective UID, and contains a
0600 `.kube-ovn-recovery-owner` marker binding the plan digest, operation ID,
member name, and target volume UID.

The isolation proof is not a Boolean approval. It contains the same canonical
plan digest and operation ID, the complete retained member set, and a bounded
verification timestamp. The helper validates that proof again inside the
privileged workload and scans the host process table before the executor opens
the target root. A missing, stale, partial, or mismatched proof fails closed.
The workload does not delete retained PVCs, stop arbitrary host processes, or
create a random cluster identity; those actions remain outside this boundary
and require the controller's durable approval and fencing protocol.
