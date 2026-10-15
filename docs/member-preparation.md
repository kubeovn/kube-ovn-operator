# New central member preparation

`internal/prepare` and the Linux `member-prepare` command prepare local files
for a new central member. The [Kubernetes workload adapter](member-workloads.md)
persists the handoff to a preparation Job and explicit launcher. The persistent
NB/SB lifecycle executor is still required; default dynamic membership remains disabled.

## Plan and invocation

Persist a plan before invoking the helper. Its operation UUID identifies one
preparation for a stable member slot and installation UID. The caller must bind
that slot to the intended Kubernetes workload/node/volume and use validated
live database inventory. Deployment-time `NODE_IPS` is not membership evidence.

```json
{
  "formatVersion": 1,
  "installationUID": "installation-uid",
  "operationID": "eeee0000-1111-2222-3333-444444444444",
  "memberName": "central-replacement",
  "databases": [
    {
      "name": "OVN_Northbound",
      "clusterID": "aaaa0000-1111-2222-3333-444444444444",
      "localAddress": "tcp:192.0.2.4:6643",
      "remoteAddresses": ["tcp:192.0.2.1:6643", "tcp:192.0.2.2:6643"]
    },
    {
      "name": "OVN_Southbound",
      "clusterID": "bbbb0000-1111-2222-3333-444444444444",
      "localAddress": "tcp:192.0.2.4:6644",
      "remoteAddresses": ["tcp:192.0.2.1:6644", "tcp:192.0.2.2:6644"]
    }
  ]
}
```

Both databases are required. UUIDs must be full, canonical and nonzero. RAFT
addresses must use canonical literal IPs; hostname resolution and credentials
are not part of this helper. Each database requires distinct remotes using the
local address's protocol. A local endpoint cannot also be a remote, and NB/SB
cannot share one local listener.

Run with an already provisioned private persistent root owned by the helper's
OS user and a compatible native tool:

```bash
member-prepare --root /var/lib/kube-ovn-members \
  --request /etc/member-plan/plan.json \
  --ovsdb-tool /usr/bin/ovsdb-tool
```

Output JSON contains the canonical plan's SHA-256 and each database's name,
cluster UUID, generated server UUID, local address and path relative to the root:
`<operationID>/ovnnb_db.db` or `<operationID>/ovnsb_db.db`. Persist these identities
before permitting startup. Standard OVS generates SID during `join-cluster`;
the helper does not assume patched tools' `--sid` support or infer SID from a
Pod/node name. The request must be a regular JSON file of at most 16 KiB.

The operator image includes `/member-prepare`, but its distroless runtime has
no native OVS tools. A production workload provisioner must still arrange a
trusted tool-containing execution environment; the binary alone does not deploy
or authorize that provisioner.

## Persistence and retry

The root must support flock, atomic rename and file/directory fsync, and must be
writable only by its OS owner. All preparation actors must honor the same lock.
Symlinked operation/record/database paths and multiply linked files are rejected.
A root descriptor is passed to subprocesses so they do not resolve the mount
pathname again. Native commands have bounded deadlines and metadata output.

An immutable `request.json` is atomically published before creating either
database. Only `join-cluster --cid=...` creates files. Each is checked with native
metadata commands and synced; `prepared.json` is published after both pass.
Failure between NB and SB, or after native file creation but before its return,
leaves existing files in place. Retrying the same plan verifies and reuses their
native identities. Equivalent database/remote ordering is accepted.

An existing operation directory without a valid request is rejected. Changed
installation/member identities, CIDs or addresses are rejected. Corrupt,
incompatible or replaced files and invalid result records remain available for
investigation. The helper never deletes an existing database, converts recovery
data, wipes storage or bootstraps a cluster. Only the current invocation's
unpublished request/JSON temporary files may be removed.

After joining, RAFT files naturally change. A completed preparation retry checks
the saved native identities instead of demanding the original content hash.
The plan digest binds inputs; it is not an approval signature or proof of quorum.
A privileged actor replacing storage or editing both data and trusted records
is outside this storage contract.

## Required lifecycle integration

The future executor must hold both operation Leases, verify live NB/SB quorum,
cluster identities and backups, persist the preparation request/result, then
authorize startup. Both new members must reach the committed fence before old
members are removed or the old workload changes. Partial NB/SB convergence is
not permission to advance. This helper alone never authorizes cluster changes.

Do not run unmodified upstream `start-db.sh` recovery logic over these files.
The pinned v1.16.13 script can archive an unjoined file after 120 seconds and has
bootstrap/recovery branches. The new-member launcher must use explicit prepared
files and must not turn missing quorum or slow joining into automatic rebuilding.
The explicit `/member-run` launcher now validates and holds these files while
supervising both databases and northd; see [the launch contract](member-launch.md).
Storage mounts, health probes, Kubernetes ownership and startup authorization
still belong to the workload provisioner and persistent lifecycle executor.

## Validation scope

Native tests cover partial attempts, interruption after file creation, restart,
immutable plan matching, corruption, changed SID, link attacks and retention of
rejected files. A live integration uses actual OVN NB/SB schemas and two existing
three-member clusters, prepares and starts a fourth member, then checks quorum,
row UUIDs/map data and stable identities on retry after joining. This does not
claim Kubernetes node replacement or complete network/lifecycle acceptance.
