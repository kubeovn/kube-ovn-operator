# Helm chart

The chart installs the operator Deployment, ServiceAccount, RBAC and CRDs.
The same CRDs are kept in `config/crd/bases` for kustomize users so that
upgrades preserve Kubernetes API conversion and status ownership.

```bash
helm lint charts/kube-ovn-operator
helm upgrade --install kube-ovn-operator charts/kube-ovn-operator \
  --namespace kube-ovn-operator-system --create-namespace
```
