#!/usr/bin/env bash
set -euo pipefail

cluster_name="kube-ovn-operator-e2e"
created_cluster=0
cleanup() {
  if [[ -n "${operator_pid:-}" ]]; then
    kill "${operator_pid}" 2>/dev/null || true
  fi
  if [[ "${created_cluster}" == 1 ]]; then
    kind delete cluster --name "${cluster_name}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster_name}"; then
  kind create cluster --name "${cluster_name}" --wait 60s
  created_cluster=1
fi
kubectl apply -k config
go run ./cmd/operator --leader-elect=false >/tmp/kube-ovn-operator-e2e.log 2>&1 &
operator_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:8081/readyz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl --fail --silent http://127.0.0.1:8081/readyz >/dev/null 2>&1; then
  cat /tmp/kube-ovn-operator-e2e.log
  exit 1
fi
kubectl apply -f config/samples/operator_v1alpha1_kubeovninstallation.yaml
if ! kubectl wait --for=jsonpath='{.status.phase}'=Ready kubeovninstallation/kube-ovn -n kube-ovn-operator-system --timeout=120s; then
  kubectl get kubeovninstallation/kube-ovn -n kube-ovn-operator-system -o yaml
  cat /tmp/kube-ovn-operator-e2e.log
  exit 1
fi
kubectl get configmap kube-ovn-operator-plan-kube-ovn -n kube-ovn-operator-system -o jsonpath='{.data.plan\.json}' | grep -q 'ValidateVersionContract'
