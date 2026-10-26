# Helm chart

The chart installs the operator Deployment, ServiceAccount, RBAC and CRDs.
The same CRDs are kept in `config/crd/bases` for kustomize users so that
upgrades preserve Kubernetes API conversion and status ownership.

```bash
helm lint charts/kube-ovn-operator
helm upgrade --install kube-ovn-operator charts/kube-ovn-operator \
  --namespace kube-ovn-operator-system --create-namespace
```

The chart leaves destructive fencing disabled by default. To enable the
production transport, set `fenceAgent.enabled=true`, provide a digest-pinned
`fenceAgent.image` containing `/fence-agent`, and configure the endpoint and
Secret names. The operator client Secret must contain `tls.crt` and `tls.key`,
while the client-CA Secret contains `ca.crt`; each rendered central workload receives a sidecar that mounts the
OVN Unix socket, a server certificate Secret and the client CA Secret. The
sidecar reads the installation's central operation Lease through the rendered
RoleBinding and clears its fence when Lease observation expires. Use an
endpoint template with `{pod}` and, for host-network central Pods, `{node}`;
the latter is resolved from the live Pod IP before every mutation.
