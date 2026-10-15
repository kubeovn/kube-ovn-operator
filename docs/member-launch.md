# Prepared central member launcher

`/member-run` is a Linux foreground supervisor for one explicitly prepared OVN
central member. It starts separate NB/SB `ovsdb-server` processes and
`ovn-northd`. It never runs `ovn-ctl` or upstream `start-db.sh`, creates a missing
database, bootstraps a cluster, archives a slow join or removes stored data.

The [Kubernetes workload adapter](member-workloads.md) persists and creates the
preparation and launch workloads. The persistent lifecycle executor is still
required. The default reconciler does not invoke this launcher or enable dynamic
membership. Starting a joining server changes live membership, so the caller
must authorize startup under both installation Leases after verifying the live
NB/SB inventory, quorum and backups. A launch request is input from that trusted
caller, not independent approval or a transport-level fencing token.

## Launch request

Persist the preparation plan and returned result in the lifecycle operation
before authorizing the workload. The launch request is a JSON object containing:

| Field | Meaning |
| --- | --- |
| `formatVersion` | Must be `1`. |
| `preparation` | The complete plan passed to `/member-prepare`. |
| `prepared` | The exact returned result, including both generated server UUIDs. |
| `northboundAddress` | Literal-IP client listener, such as `tcp:192.0.2.4:6641`. |
| `southboundAddress` | Literal-IP client listener, such as `tcp:192.0.2.4:6642`. |
| `northboundRemotes` | Explicit NB client endpoints for northd, including this member and peers. |
| `southboundRemotes` | Explicit SB client endpoints for northd, including this member and peers. |
| `tls` | Required when any client or RAFT listener uses SSL; omit for TCP-only launch. |

Each remote list contains 2–8 distinct canonical literal-IP endpoints and uses
the corresponding client listener's protocol. Client ports are separate from
RAFT ports; they must come from the lifecycle executor's validated inventory.
The launcher never derives client ports from RAFT addresses. All four local
NB/SB client/RAFT listeners must have different endpoints.

Northd connects to the explicit peer lists because NB and SB leaders may be on
different nodes and the new member normally starts as a follower. Each native
connection string also contains `cid:<expected-cluster-UUID>`. Endpoints are
sorted before adding the cluster UUID, for compatibility with native parsers
that require sorted input when removing the `cid:` option.

The optional `tls` object contains absolute mounted paths only:

```json
{
  "privateKey": "/var/run/tls/key",
  "certificate": "/var/run/tls/cert",
  "ca": "/var/run/tls/cacert"
}
```

The existing files must be nonempty regular files; projected Secret symlinks are
supported. The native tools receive explicit key, certificate and CA arguments.
The launcher does not generate credentials, bootstrap a CA or put credential
contents into output. Native tools remain responsible for parsing certificates
and enforcing TLS. Real TLS interoperability is a separate acceptance item.

Run in a trusted image containing compatible native tools:

```bash
/member-run \
  --root /var/lib/ovn-member \
  --run-dir /var/run/ovn \
  --request /etc/ovn-member/launch.json
```

`--ovsdb-tool`, `--ovsdb-server` and `--ovn-northd` accept absolute executable
paths, or resolve from `PATH` when omitted. The default `operator` image contains
the launcher binary but does not contain those native tools. Build the separate
`central-runtime` target for a complete execution image:

```bash
docker build --target central-runtime -t kube-ovn-central-runtime:local .
```

That target adds both helpers to the published Kube-OVN v1.16.13 image pinned at
`sha256:03ca6c879a95a73faa12d9531a8fbfc7b2f5f038c939847fa19a5dda2e425e2b`.
It runs as UID/GID 65532 and starts `/member-run` directly. The provisioner must
provide the request, private directories owned by that UID, storage and required
network placement. The native runtime version is fixed by this image; do not
treat it as compatible with arbitrary installation versions. Publication, version
selection and Kubernetes deployment still belong to the lifecycle integration.
The upstream image's `ovsdb-server` has a file capability. The runtime installs
ordinary executable copies under `/opt/ovn-native` without preserving capability
attributes and puts that directory first in `PATH`. The launcher rejects setuid,
setgid and capability-bearing tool files, because privileged exec can clear the
parent-death signal. The copies retain the original native binary contents.

CI builds the derived `central-test` target and runs the real native lifecycle
tests as UID/GID 65532 inside it. The test binary and tool-directory symlinks are
absent from `central-runtime`. Container builds run in CI, not local validation.
The image test also verifies that the original capability-bearing server is
rejected; it does not relax process-inspection checks or grant extra capabilities.

## Storage, process ownership and restart

The persistent and runtime roots must already exist, be private real directories
owned by the invoking OS user, and support local process locks. Startup acquires
the preparation root lock, validates the immutable request/result and checks
both native files against the recorded CID, SID and address. Missing records or
files fail startup; they are never recreated by the launcher. A separate runtime
lock excludes another member using the same sockets and PID files. Existing
symlinked or hard-linked runtime objects are rejected.

Children use pinned directory descriptors for database paths and runtime files.
Their working directory is the private runtime directory, so native crash
artifacts do not land in the supervisor's caller directory.
They inherit both locks and must retain the inherited descriptors. The supported
native processes are started in foreground mode without monitoring or daemonizing.
The supervisor keeps their creating OS thread alive and configures Linux's
parent-death SIGKILL. If the supervisor is killed, its native children terminate
and release their inherited locks. This does not fence an already accepted
remote command or a privileged actor that rewrites the trusted storage.
After a crash, native threads may briefly retain those locks even after their
leader appears as a zombie. A new attempt must wait for actual lock release;
it must never unlink a lock file to bypass an outstanding owner.

Any native child exit, including a clean exit, stops the entire member and
returns an error. SIGINT/SIGTERM cancellation signals all children with SIGTERM,
then uses SIGKILL after `--shutdown-timeout` (default 10 seconds, maximum 30).
The supervisor reaps every started child before returning and releasing its
own handles. Native stdout/stderr stream directly to container logs. The caller
may restart the same authorized operation against the retained files; it must
not replace an uncertain outcome with a newly generated operation or database.

The runtime socket/PID names match the existing observation adapters when
`--run-dir=/var/run/ovn`: `ovnnb_db.sock`, `ovnsb_db.sock`, their fixed `.ctl`
sockets, and northd's default PID-specific control socket. `ovn-appctl -t
ovn-northd` works through the normal northd PID file. Process existence is not
readiness: joining or disconnected databases remain running without triggering
automatic rebuilding. The controller must observe current identities, quorum,
both committed-data fences, northd and network health before proceeding.

## Validation and remaining integration

Native integration tests prepare a fourth member for real three-member NB/SB
clusters, start it through the production launcher, verify all members caught up,
preserve row UUIDs/map data, and observe northd compiling a logical switch into
the SB datapath. They check inherited storage/runtime descriptors, exclusive
startup, graceful restart, child failure, supervisor SIGKILL and restart after
that crash. Tests also cover missing/changed preparation records and files,
invalid launch inputs, partial process startup and SIGKILL escalation.

The workload adapter now binds Node/volume/workload identities and creates the
Job/StatefulSet. Runtime image publication/selection, automatic storage provisioning,
probe configuration, live startup authorization and the persistent joint NB/SB
replacement state machine remain open. These native tests do not exercise Kubernetes node replacement,
TLS, a complete Kube-OVN installation or packet delivery after a transition.
