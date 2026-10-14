#!/usr/bin/env bash
set -euo pipefail

cluster_name="kube-ovn-operator-e2e"
created_cluster=0
cleanup() {
  if [[ -n "${operator_pid:-}" ]]; then
    kill "${operator_pid}" 2>/dev/null || true
  fi
  kubectl delete configmap kube-ovn-operator-workload-smoke kube-ovn-operator-external-owner \
    -n kube-ovn-operator-system --ignore-not-found >/dev/null 2>&1 || true
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
if ! kubectl wait --for=condition=Ready kubeovninstallation/kube-ovn -n kube-ovn-operator-system --timeout=120s; then
  kubectl get kubeovninstallation/kube-ovn -n kube-ovn-operator-system -o yaml
  cat /tmp/kube-ovn-operator-e2e.log
  exit 1
fi
kubectl get configmap kube-ovn-operator-plan-kube-ovn -n kube-ovn-operator-system -o jsonpath='{.data.plan\.json}' | grep -q 'ValidateVersionContract'
kubectl get configmap kube-ovn-operator-plan-kube-ovn -n kube-ovn-operator-system -o jsonpath='{.data.values\.yaml}' | grep -q 'installMode: full'
kubectl get lease kube-ovn-operator-installation-kube-ovn -n kube-ovn-operator-system >/dev/null
kubectl get lease kube-ovn-operator-central-kube-ovn -n kube-ovn-operator-system >/dev/null

installation_uid=$(kubectl get kubeovninstallation/kube-ovn -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')
go run ./test/e2e/apply-smoke --namespace kube-ovn-operator-system --installation kube-ovn --installation-uid "${installation_uid}"
kubectl get configmap kube-ovn-operator-workload-smoke -n kube-ovn-operator-system -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}' | grep -qx 'kube-ovn-operator'
kubectl get configmap kube-ovn-operator-workload-smoke -n kube-ovn-operator-system -o jsonpath='{.metadata.labels.operator\.kubeovn\.io/installation-uid}' | grep -qx "${installation_uid}"
go run ./test/e2e/apply-smoke --external-owner --name kube-ovn-operator-external-owner --namespace kube-ovn-operator-system --installation kube-ovn --installation-uid "${installation_uid}"
kubectl delete configmap kube-ovn-operator-workload-smoke kube-ovn-operator-external-owner -n kube-ovn-operator-system --ignore-not-found >/dev/null
