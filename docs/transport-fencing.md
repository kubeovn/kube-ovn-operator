# Transport fencing contract

The operator uses Kubernetes Leases to coordinate installation and central
operations. A Lease check in the operator stops later writes, but it cannot
retract an `exec` or native request that a remote process has already accepted.
This document defines the additional capability required for a membership
mutation to be considered strictly fenced.

## Token

`lease.Token(ctx, lease.Central)` returns a `lease.FenceToken` containing:

- the installation UID;
- the Lease UID;
- the Lease scope (`central` for OVN membership changes);
- the current holder identity; and
- the Lease transition number, used as the fence epoch.

The token is an operation identity, not a signature or an approval. The remote
executor must bind the same identity to its current server-side fence. Lease
renewals keep the epoch unchanged. A takeover changes the holder and increments
the epoch; deleting and recreating the Lease changes its UID as well.

## Client boundary

`ovnadmin.CommandClient` requires `ovnadmin.FencedRunner` for `cluster/leave`
and `cluster/kick`. The interface is deliberately separate from the
observation-only `Runner`:

```go
RunFenced(context.Context, string, []string, lease.FenceToken) ([]byte, error)
```

The membership Pod adapter validates the pinned Pod/container identity, then
forwards only to a `health.FencedCommandRunner`. A plain `Runner`, including
the standard Kubernetes `pods/exec` implementation, returns
`ovnadmin.ErrFencingUnavailable` before dispatch. This is fail-closed because
stream cancellation is not remote revocation.

The operator can use the HTTP adapter by setting
`--fence-endpoint-template` to a trusted HTTPS URL template. The supported
placeholders are `{namespace}`, `{pod}`, `{container}`, and `{node}`. The
first three are escaped URL path segments. `{node}` is an optional host
placeholder resolved from the live central Pod IP or its Node InternalIP,
which allows a host-network sidecar to be addressed without a shared proxy.
The adapter sends
`POST <expanded-template>/v1/fence/execute` with this bounded JSON body:

```json
{
  "command": ["ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", "cluster/leave", "OVN_Northbound"],
  "token": {
    "installationUID": "...",
    "leaseUID": "...",
    "scope": "central",
    "holder": "...",
    "epoch": 4
  }
}
```

The response is a JSON object containing `output` on success. A response with
`code: stale_fence` or `code: fence_not_armed` is surfaced as
`ovnadmin.ErrStaleFence`, which forces a fresh observation before retrying.
Loopback HTTP is accepted only for local tests; production endpoints must use
HTTPS and credentials must come from the transport's TLS configuration, never
from the URL.

The reusable `internal/fencing.Gate` and `internal/fencing.Handler` implement
the server-side compare-and-admit critical section. `Gate.Arm` and
`Gate.Disarm` are intentionally outside the HTTP handler: an authoritative
Lease watcher or control-plane agent must call them, and the handler has no
endpoint that lets a request arm its own token. The handler also bounds request
and response sizes, requires a token for every mutation, restricts its default
validator to canonical `ovs-appctl cluster/leave` and `cluster/kick` commands,
and returns distinct stale/not-armed error codes. The optional agent requires
mutual TLS: set `--tls-client-ca-file` to a mounted CA bundle; it uses
`RequireAndVerifyClientCert` with TLS 1.3 or newer. The operator must provide
`--fence-ca-file`, `--fence-client-cert-file`, and `--fence-client-key-file`
alongside `--fence-endpoint-template`. The native process supervisor remains a
deployment responsibility.

The optional `fence-agent` image target (`docker build --target fence-agent`)
combines the Handler with a non-shell `ovs-appctl` executor and observes one
named central Lease. The Operator Helm renderer can inject this image as a
sidecar into each rendered `ovn-central` Deployment or StatefulSet. The
sidecar shares the `/var/run/ovn` socket volume, mounts the named server TLS
and client-CA Secrets, receives a namespace-scoped Lease reader RoleBinding,
and uses the deterministic central Lease name. Its installation UID can be
derived from the Lease owner reference; an explicit `--installation-uid`
remains available for independently deployed agents. It clears the Gate on
missing, expired or unreadable Lease state, so an observation failure fails
closed. The resource-version watcher also arms an expiry timer so a quiet
Lease cannot remain armed after its renewal deadline. Production acceptance
still needs a cluster-level test proving that Lease takeover and command
admission are fenced by the chosen control-plane design.

The same agent exposes `POST /v1/fence/restore` for destructive NB/SB restore.
The operator sends the verified backup as the request body, the database name in
`X-Kube-OVN-Database`, and the complete Lease token in authenticated headers.
The agent accepts only `OVN_Northbound` or `OVN_Southbound`, first stages the
complete body in a private temporary file, then streams that verified file to
the fixed `ovsdb-client restore` invocation. This prevents an unknown-length
oversized request from feeding a partial snapshot to the native process. The
Gate comparison remains in the same critical section as native restore
admission. The injected sidecar mounts the OVN database sockets and uses the
same central Lease identity as the operator; its renderer configuration also
creates the namespace-scoped Lease reader binding.
The agent also serves `GET /healthz` for liveness and `GET /readyz` for
readiness. Readiness is `503` until the Lease watcher has armed a current
fence, and becomes `200` only while that fence remains armed. Restore requests
with a declared `Content-Length` larger than 1 GiB are rejected before staging;
unknown-length requests are read into the same bounded temporary file and
rejected before the native executor is called when they exceed 1 GiB.

## Remote executor requirements

A production remote executor must implement `RunFenced` with the following
semantics:

1. Read the currently armed fence for the target operation and installation.
2. Compare every token field, including Lease UID, holder and epoch, in the
   same serialized critical section that admits the OVN mutation.
3. Reject a stale, missing, revoked or scope-mismatched token without starting
   `ovs-appctl`, `ovsdb-client` or an equivalent native command.
4. Keep the comparison and command admission atomic with respect to fence
   replacement. A check followed by an independent command launch is
   insufficient because a Lease takeover can occur between the two steps.
5. Return a distinct stale-fence error. The operator treats an accepted request
   with an uncertain result as requiring fresh observation before retrying.

The remote service may replicate the current epoch to a sidecar, native admin
daemon or admission proxy, but it must make the compare-and-admit operation
atomic from the perspective of the command. Merely adding token values to the
command arguments, checking the Lease again in the operator, or relying on
HTTP/SPDY cancellation does not satisfy this contract.

## Current status

The operator-side token, HTTP client, reusable server gate/handler,
fail-closed behavior and unit tests are implemented. The repository also
contains a `fence-agent` reference binary that wires `Gate.Arm` to the named
Lease, requires mutual TLS, and invokes the fixed native membership and restore
commands. The Helm renderer and Chart wiring now support deploying that agent
beside each central workload with the OVN socket, Lease reader binding, and
certificate mounts required by the contract. The remaining production work is
deploying it in a real cluster, configuring the endpoint template and
credentials, and proving Lease takeover, membership mutation, and destructive
restore behavior across multiple nodes. Until that deployment is active and
the remote service has passed readiness, membership leave/kick and destructive
restore remain intentionally unavailable to the normal `pods/exec` path.
