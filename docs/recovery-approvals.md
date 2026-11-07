# Destructive recovery approvals

`KubeOVNRecoveryApproval` is a human-created, namespace-scoped authorization for
a destructive recovery operation. It is separate from
`KubeOVNRecoveryRequest`: a request describes the desired operation, while the
approval records the independent decision to permit a restore when the normal
OVN quorum evidence is unavailable.

## What an approval binds

The `spec` is immutable after creation. It must bind all of the following to the
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
`approvedBy` field on a recovery request is audit input only; it never grants
authority.

The `revoked` field can be changed from `false` to `true` once. Revocation
cannot be reversed. An expired or revoked approval cannot start a new operation.

## Lifecycle

The caller must store the approval name and UID in the admitted rollback or
recovery plan. Before any database write, the operator:

1. rereads the approval and validates every binding and its expiry;
2. reserves it by writing `status.phase=Reserved` and a deterministic
   `status.decisionDigest`;
3. executes the already admitted, fenced operation;
4. writes `status.phase=Completed` only after both databases and the target
   workloads converge.

The reservation is retryable for the same immutable decision. A completed
approval is one-time use and cannot authorize another operation. A lost Lease,
changed identity, changed topology, changed backup or changed Chart digest
stops the operation and leaves the approval unusable for that different target.

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
an optional mTLS `fence-agent` reference deployment, but production deployment
and multi-node takeover/restore acceptance are still required. Until a live
fence endpoint passes readiness, the operator remains fail-closed even when an
approval exists.

The provider and binding rules are covered by unit tests. A real lost-quorum
restore still requires the configured fenced executor and a multi-node
acceptance test.
