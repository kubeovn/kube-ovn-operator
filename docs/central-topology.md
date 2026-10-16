# Central topology discovery and retention

The default controller records an initial local RAFT identity inventory after
workload rollout. It requires both installation-bound Leases, an uncached client
and a complete observation of the desired central Pods. Discovery itself does
not select or start a membership transition; the controller's lifecycle selector
may admit the safe one-member reduction described below. HCP, external databases and standalone mode use
their existing health paths and do not establish this local RAFT baseline.

Discovery follows the Pod/ReplicaSet/workload owner UID chain to the installation.
It reads each server's full CID, SID and RAFT address from native `cluster/status`,
then uses the admin transport to verify the leader, every configured peer, their
terms, committed/applied positions and live connections. Abbreviated member IDs
cannot create identities. The full configured member set must equal the observed
Pod set, and every desired member must be caught up.

Client addresses come from `ovsdb-server/list-remotes`. Database-backed remotes
are resolved through the actual NB_Global/SB_Global `connections` references and
Connection rows; unreferenced rows cannot establish a listener. A unique passive
TCP or TLS listener must match the observed RAFT host address, which must be a
Node InternalIP. Unix sockets and outgoing remotes are not client listeners.
No default client port is inferred from a RAFT address. This discovers TLS
endpoints; it does not implement certificate or Secret lifecycle management.

The record binds Node UID, owning workload kind/name/UID, and database storage.
The supported database roots are the Chart's `/etc/ovn` and the member runtime's
`/var/lib/ovn-member`. Host paths bind the Node and path; filesystem claims bind
both PVC and PV UIDs and the PV claim reference. Temporary storage, ambiguous
roots, dynamic subpaths, unbound claims and shared member storage are rejected.
This records Kubernetes storage identity, not on-disk file ownership or exclusive
access authorization. Lifecycle preparation and startup still enforce those.

The ConfigMap has a deterministic name per installation UID, is immutable and
has no owner reference. Its data includes installation UID/generation and the
first complete topology, including the observed Pod UID and container ID.
`status.topology.inventory` pins the ConfigMap name, UID and SHA-256 digest.
Before reapplying workloads, the controller verifies any retained baseline and
blocks Apply on identity or observation failure. An absent baseline permits the
first rollout. An accepted create followed by a lost response or failed status write recovers
the same record. A pinned record that disappears is not recreated. Its reference
is retained on observation failures; old health evidence is still cleared.

Subsequent discovery compares stable native, Node, workload and storage
identities. Fresh Pod/container identities can differ after a normal restart.
The original endpoint remains historical provenance and must be rebound to live
Pods before any operation; the baseline alone never authorizes an admin command.
Terms and indexes are intentionally excluded from this identity record, so normal
writes and elections do not accumulate immutable ConfigMaps.

`TopologyRecorded` is true only after fresh discovery and retained-record
verification. It is an additional Ready gate, not a replacement for OVN, runtime
or network checks. Complete discovery currently requires all desired members;
loss of a member preserves the inventory and blocks verification. A lifecycle
operation uses that retained identity as its original inventory. The default
controller resumes such an operation before Chart rendering, and only a
validated immutable completion receipt plus fresh final discovery can create a
successor baseline. The safe operation selector obtains fresh quorum, backup and
approval evidence through the lifecycle executor before any mutation.

Baseline transitions require explicit lifecycle completion evidence; this module
does not absorb external topology changes automatically. The default controller
can select one safe replica reduction or an explicit same-size replacement. An
implicit reduction requires an exactly one-member independent StatefulSet
pinned successfully. A Chart Deployment reduction or replacement must set
`central.memberRequests[].retireEndpoint` or `replaceEndpoint` to bind one exact
Pod; admission pins the Deployment and ReplicaSet UIDs, target Pod UID, all
survivor UIDs and the admitted spec before any database or scale-down mutation.
Ambiguous reductions and automatic target inference remain blocked. The
selected path can authorize its next apply checkpoint only after the immutable
completion receipt and successor baseline are pinned; strict in-flight fencing
remains open. Joint observations are samples, not an atomic NB/SB snapshot.
Retained metadata assumes trusted Kubernetes writers and cannot authenticate
against an actor able to rewrite both records and status.

Tests cover complete and partial discovery, peer health/configuration disagreement,
listener resolution and changes, identity replacement, missing/corrupt records,
normal restart, commit advancement and lost create responses. Native OVSDB tests
exercise database-backed listener resolution. The kind member test records the
real final member after joint replacement and verifies its Node/workload/PVC/PV
and NB/SB identities plus reference persistence through the installed CRD. This
remains a one-node lifecycle test, not multi-node HA or network acceptance.
