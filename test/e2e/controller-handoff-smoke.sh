#!/usr/bin/env bash
set -euo pipefail

cluster_name="kube-ovn-operator-controller-e2e"
test_dir=$(mktemp -d)
created_cluster=0
operator_pid=""
server_pid=""

cleanup() {
  if [[ -n "${operator_pid}" ]]; then
    kill "${operator_pid}" 2>/dev/null || true
    wait "${operator_pid}" 2>/dev/null || true
  fi
  if [[ -n "${server_pid}" ]]; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  if [[ "${created_cluster}" == 1 ]]; then
    kind delete cluster --name "${cluster_name}" >/dev/null 2>&1 || true
  fi
  rm -rf "${test_dir}"
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster_name}"; then
  kind create cluster --name "${cluster_name}" --config test/e2e/kind-controller.yaml --wait 120s
  created_cluster=1
fi

kubectl apply \
  -f config/namespace.yaml \
  -f config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml \
  -f config/crd/bases/operator.kubeovn.io_kubeovnrecoveryrequests.yaml \
  -f config/crd/bases/operator.kubeovn.io_kubeovnmemberremovalapprovals.yaml \
  -f config/rbac/role.yaml \
  -f config/rbac/service_account.yaml >/dev/null

docker build --target central-runtime -t kube-ovn-central:e2e . >/dev/null
docker build -f test/e2e/controller-fixture.Dockerfile -t kube-ovn-central-fixture:e2e . >/dev/null
kind load docker-image --name "${cluster_name}" kube-ovn-central-fixture:e2e

helm package test/e2e/controller-chart --destination "${test_dir}" >/dev/null
archive="${test_dir}/kube-ovn-1.17.0.tgz"
digest="sha256:$(sha256sum "${archive}" | cut -d ' ' -f1)"
python3 -m http.server 18084 --bind 127.0.0.1 --directory "${test_dir}" >"${test_dir}/chart-server.log" 2>&1 &
server_pid=$!

go build -o "${test_dir}/operator" ./cmd/operator
backup_dir="${test_dir}/backups"
mkdir -m 0700 "${backup_dir}"
"${test_dir}/operator" \
  --leader-elect=false \
  --metrics-bind-address=0 \
  --health-probe-bind-address=:18083 \
  --backup-dir="${backup_dir}" \
  >"${test_dir}/operator.log" 2>&1 &
operator_pid=$!

for _ in $(seq 1 90); do
  if curl --fail --silent http://127.0.0.1:18083/readyz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18083/readyz >/dev/null

kubectl apply -f - <<YAML
apiVersion: operator.kubeovn.io/v1alpha1
kind: KubeOVNInstallation
metadata:
  name: controller-retirement
  namespace: kube-ovn-operator-system
spec:
  version: v1.17.0
  chart:
    source: http://127.0.0.1:18084/kube-ovn-1.17.0.tgz
    digest: ${digest}
  mode: controlPlaneOnly
  central:
    mode: raft
    replicas: 3
    dynamicMembership: true
    placement:
      nodeSelector:
        node-role.kubernetes.io/control-plane: ""
YAML

for _ in $(seq 1 180); do
  ready=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}' 2>/dev/null || true)
  if [[ "${ready}" == 3 && -n "${inventory}" ]]; then
    break
  fi
  sleep 2
done
if [[ "$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}')" != 3 ]]; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
  exit 1
fi
inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
if [[ -z "${inventory}" ]]; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
  exit 1
fi

worker=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[0].metadata.name}')
worker_ip=$(kubectl get node "${worker}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
claim_name=controller-replacement-data
volume_name=controller-replacement-volume
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${claim_name}
  namespace: kube-ovn-operator-system
spec:
  storageClassName: ""
  volumeName: ${volume_name}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
YAML
claim_uid=""
for _ in $(seq 1 60); do
  claim_uid=$(kubectl get pvc "${claim_name}" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [[ -n "${claim_uid}" ]]; then
    break
  fi
  sleep 1
done
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${volume_name}
spec:
  storageClassName: ""
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    name: ${claim_name}
    namespace: kube-ovn-operator-system
    uid: ${claim_uid}
  hostPath:
    path: /tmp/kube-ovn-operator-controller-replacement
    type: DirectoryOrCreate
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: [${worker}]
YAML
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/${claim_name} -n kube-ovn-operator-system --timeout=60s >/dev/null

fixture_id=$(docker image inspect kube-ovn-central-fixture:e2e --format '{{.Id}}')
fixture_digest=${fixture_id#sha256:}
member_image="kube-ovn-central-fixture@sha256:${fixture_digest}"
target_pod=$(kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o jsonpath='{.items[0].metadata.name}')
if [[ -z "${target_pod}" ]]; then
  echo 'no central Pod was available for the replacement request' >&2
  exit 1
fi

cat >"${test_dir}/replacement.json" <<JSON
{"spec":{"central":{"memberRequests":[{"name":"replacement","replaceEndpoint":"${target_pod}","nodeName":"${worker}","claimName":"${claim_name}","volumeName":"${volume_name}","image":"${member_image}","northboundAddress":"tcp:${worker_ip}:6643","southboundAddress":"tcp:${worker_ip}:6644","northboundClientAddress":"tcp:${worker_ip}:6641","southboundClientAddress":"tcp:${worker_ip}:6642"}]}}}
JSON
kubectl patch koi/controller-retirement -n kube-ovn-operator-system --type=merge --patch-file "${test_dir}/replacement.json" >/dev/null

request_name=""
for _ in $(seq 1 90); do
  request_name=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.requestName}' 2>/dev/null || true)
  if [[ -n "${request_name}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${request_name}" ]]; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
  exit 1
fi

journal_name=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.journalName}')
request_json=$(kubectl get configmap "${request_name}" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
journal_json=$(kubectl get configmap "${journal_name}" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "${request_json}" | grep -Fq "${target_pod}"
printf '%s\n' "${request_json}" | grep -Fq '"kind":"Deployment"'
printf '%s\n' "${request_json}" | grep -Fq '"replicaSet"'
printf '%s\n' "${journal_json}" | grep -Fq "${request_name}"

replicas=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "${replicas}" != 3 ]] || ! kubectl get pod "${target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo 'Deployment changed before lifecycle execution was authorized' >&2
  exit 1
fi
echo "default controller admitted Deployment replacement request ${request_name} for ${target_pod} and persisted the journal"
