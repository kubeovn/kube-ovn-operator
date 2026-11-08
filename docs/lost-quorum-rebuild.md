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
