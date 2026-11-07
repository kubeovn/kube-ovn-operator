# Central member workload provisioning

`internal/workload` is the Kubernetes adapter between a lifecycle operation and
the existing native preparation/launch helpers. It provides two operations:

- `Prepare(ctx, Request)` reserves the request and storage, creates or observes
  the preparation Job, and returns a result only after its native output and
  producer identity have been stored in an immutable record.
- `Start(ctx, Reference, central.Request)` requires that recorded result and
  persists the exact launch request before creating the headless Service and
  single-member StatefulSet. It returns the workload name and UID, not a health
  or membership-convergence claim.

Both require an uncached client and the same installation's installation and
central-membership Leases. The caller must persist the returned record reference.
It must not reconstruct an operation from a Pod name, substitute a new operation
UUID after an uncertain outcome or infer live membership from bootstrap IPs.

## Durable identities and ordering

The member's immutable reservation includes the installation UID and generation,
operation UUID, stable member name, full NB/SB cluster IDs and RAFT addresses,
Node name/UID, PVC name/UID, PV name/UID, image digest and workload configuration.
The record name is deterministic for the installation UID and member slot. A
different request for that slot is rejected instead of replacing the record.

The PVC receives a reservation annotation containing the member record's UID.
Updates use resourceVersion so competing members cannot claim the same PVC,
including members protected by different installation Leases. Existing active
consumers are rejected unless they belong to this member's verified Job or
StatefulSet. PV ClaimRef, Bound status and filesystem mode are checked alongside
the claim UID; a recreated object with the same name is not accepted.

The preparation Job mounts a separate immutable ConfigMap containing precisely
the native preparation Plan. Its standard output is written to the Kubernetes
termination-message file. The adapter accepts one successful producer only,
checks the exact container/template contract, validates the complete two-database
result and records the Job UID, Pod UID and container ID. Missing/truncated
messages, partial results, failed Jobs and ambiguous producers stop progress.
The native helper remains responsible for fsync and native file identity checks.

The launch ConfigMap contains both the accepted Plan and exact Result. Startup
rechecks the preparation producer and the current installation, Node and volume
bindings. Each created workload's UID and admitted spec digest are recorded.
Retry after an accepted create whose response was lost can recover the matching
object. Once its binding exists, absence, replacement or spec drift is an error;
the adapter does not overwrite it or create a replacement silently.

API-defaulted workload fields are retained in the spec digest. Admission may not
add sidecars, init containers, environment variables or lifecycle commands, change
the selected image/commands/mounts, or elevate the runtime's security context.
Dry-run admission runs before workload creation. Writes are followed by readback.

## Runtime and storage contract

The runtime image must be digest-pinned and contain `/member-prepare`,
`/member-run`, a shell and compatible native tools. The repository's
`central-runtime` Docker target supplies these. Publishing and selecting that
image in the installation version contract is still pending.

Storage is an existing Bound filesystem PVC. Its mounted root must already be a
private real directory owned by UID/GID 65532 and support the native preparer's
locking/fsync/rename contract. The adapter never creates or deletes PVCs/PVs,
changes their ownership, sets fsGroup or repairs data permissions. It does not
claim that an arbitrary CSI driver provides local process-lock semantics.

The Job and StatefulSet pin `nodeName` and use host networking. All local RAFT
and client listeners must use current InternalIPs of the pinned Ready Node and
unprivileged ports. The containers run as UID/GID 65532 with no additional Linux
capabilities, no privilege escalation, no API token and a read-only root
filesystem. Separate emptyDirs provide `/tmp` for native temporary files and
`/var/run` for the launcher's private runtime directory;
the persistent database root is never created or repaired during startup.

The StatefulSet contains exactly one member and uses OnDelete updates. It does
not interpret process startup as database readiness. Existing OVN observation
adapters can follow its installation owner and `app=ovn-central` Pod labels,
but the lifecycle executor must provide the complete temporary member inventory
instead of enforcing only the installation's final replica count.

TLS member requests use `central.memberRequests[].tlsSecretRef` to bind one
Secret UID and resourceVersion before preparation. The Secret must contain
either `tls.key`/`tls.crt`/`ca.crt` or the native `key`/`cert`/`cacert` keys.
The Job and StatefulSet mount those selected keys read-only at
`/etc/ovn/tls/{key,cert,ca}`, and the Pod template records the Secret
resourceVersion so a rotation cannot be silently ignored. Any Secret
replacement, rotation, deletion or key mapping change fails closed on the
next lifecycle write and requires a newly selected member request. All four
NB/SB listener and client addresses must use one protocol; `ssl` requires the
reference and `tcp` forbids it. Credentials are never copied into lifecycle
records or status.

## Limits and verification

The immutable records deliberately have no owner reference: garbage collection
of an Installation must not erase the only record of retained database identity.
Workloads have the Installation controller owner for existing observation.
Record retirement, workload removal and installation deletion protection belong
to the joint lifecycle executor and are not implemented by this adapter.

Leases and pre-write identity checks do not revoke an API or native request
already accepted remotely. Kubernetes name-based scheduling and PVC mounts also
cannot atomically bind Node/PVC UIDs. The membership mutation adapter therefore
requires `ovnadmin.FencedRunner` and forwards the current `lease.FenceToken`;
ordinary `pods/exec` is rejected for leave/kick because it has no atomic
server-side revocation. A production remote executor must bind the token to its
current fence before starting the command. Trusted API writers, admission and
storage administrators remain part of the deployment contract.

Tests cover controller restart, absent and modified native results, competing
storage consumers/reservations, Node/PVC/PV/workload identity changes, installation
generation changes, Lease loss, admission changes and lost create responses.
The kind smoke workflow additionally runs the real preparation helper and native
launcher in Pods on a retained test PVC. It joins a new NB/SB member to isolated
seed databases, verifies recorded CID/SID/address and committed data, preserves a
business row UUID/map value and repeats Start without replacing the StatefulSet.
That test uses kind's existing cluster and does not install Kube-OVN or exercise
multi-node replacement, a joint replacement state machine, TLS or network traffic.
