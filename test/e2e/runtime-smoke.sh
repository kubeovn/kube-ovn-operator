#!/usr/bin/env bash
set -euo pipefail

# The existing smoke job provisions the isolated kind cluster and API CRDs.
# This fixture exercises the real operator binary without installing a CNI.
test_dir=$(mktemp -d)
cleanup() {
  if [[ -n "${operator_pid:-}" ]]; then kill "${operator_pid}" 2>/dev/null || true; fi
  if [[ -n "${server_pid:-}" ]]; then kill "${server_pid}" 2>/dev/null || true; fi
  rm -rf "${test_dir}"
}
trap cleanup EXIT
helm package test/e2e/chart --destination "${test_dir}" >/dev/null
archive="${test_dir}/kube-ovn-1.17.0.tgz"
digest="sha256:$(sha256sum "${archive}" | cut -d ' ' -f1)"
python3 -m http.server 18082 --bind 127.0.0.1 --directory "${test_dir}" >"${test_dir}/server.log" 2>&1 &
server_pid=$!
go build -o "${test_dir}/operator" ./cmd/operator
"${test_dir}/operator" --leader-elect=false --metrics-bind-address=0 --health-probe-bind-address=:18083 >"${test_dir}/operator.log" 2>&1 &
operator_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18083/readyz >/dev/null 2>&1; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18083/readyz >/dev/null
kubectl apply -f - <<'YAML'
apiVersion: operator.kubeovn.io/v1alpha1
kind: KubeOVNInstallation
metadata:
  name: runtime-smoke
  namespace: kube-ovn-operator-system
spec:
  version: v1.17.0
  chart:
    source: http://127.0.0.1:18082/kube-ovn-1.17.0.tgz
    digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
  mode: dataPlaneOnly
  central:
    mode: raft
    replicas: 3
  dataPlane:
    externalOvnCentral:
      nbEndpoint: nb.example
      sbEndpoint: sb.example
YAML
kubectl wait -n kube-ovn-operator-system koi/runtime-smoke --for=condition=Degraded --timeout=90s
if kubectl get deployment/runtime-smoke -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo 'Digest mismatch unexpectedly created a workload' >&2
  exit 1
fi
installation_uid=$(kubectl get koi/runtime-smoke -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')
kubectl apply -f - <<YAML
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: kube-ovn-operator-installation-runtime-smoke
  namespace: kube-ovn-operator-system
  ownerReferences:
    - apiVersion: operator.kubeovn.io/v1alpha1
      kind: KubeOVNInstallation
      name: runtime-smoke
      uid: ${installation_uid}
      controller: true
spec:
  holderIdentity: runtime-smoke-competitor
  leaseDurationSeconds: 120
YAML
kubectl patch koi/runtime-smoke -n kube-ovn-operator-system --type=merge -p "{\"spec\":{\"chart\":{\"digest\":\"${digest}\"}}}"
kubectl wait -n kube-ovn-operator-system koi/runtime-smoke --for=jsonpath='{.status.upgrade.step}'=AcquireLease --timeout=90s
if kubectl get deployment/runtime-smoke -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo 'Competing holder did not block workload mutation' >&2
  exit 1
fi
kubectl patch lease/kube-ovn-operator-installation-runtime-smoke -n kube-ovn-operator-system --type=merge -p '{"spec":{"holderIdentity":""}}'

if ! kubectl wait -n kube-ovn-operator-system koi/runtime-smoke --for=condition=WorkloadsReady --timeout=180s; then
  kubectl get koi/runtime-smoke -n kube-ovn-operator-system -o yaml
  cat "${test_dir}/operator.log"
  exit 1
fi
kubectl get koi/runtime-smoke -n kube-ovn-operator-system -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' | grep -qx VerificationPending
kubectl get koi/runtime-smoke -n kube-ovn-operator-system -o jsonpath='{.status.observedVersion}' | grep -qx v1.17.0
kubectl get deployment/runtime-smoke -n kube-ovn-operator-system -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}' | grep -qx kube-ovn-operator
kubectl patch koi/runtime-smoke -n kube-ovn-operator-system --type=merge -p '{"spec":{"upgrade":{"allowRollback":true}}}'
kubectl wait -n kube-ovn-operator-system koi/runtime-smoke --for=condition=Degraded --timeout=90s
kubectl get koi/runtime-smoke -n kube-ovn-operator-system -o jsonpath='{.status.upgrade.step}' | grep -qx ApplyCheckpoint
