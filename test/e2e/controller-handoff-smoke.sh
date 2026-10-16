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

# The lifecycle request is digest-pinned. Kind loads the tag on every node,
# then we add the exact local containerd digest alias so the replacement
# StatefulSet can start on the worker without reaching a registry.
fixture_image_name=docker.io/library/kube-ovn-central-fixture:e2e
fixture_digest=""
for node in $(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
  digest=$(docker exec "${node}" /usr/local/bin/ctr -n k8s.io images ls | awk -v image="${fixture_image_name}" '$1 == image {print $3}')
  if [[ ! "${digest}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    echo "could not read fixture image digest on ${node}" >&2
    exit 1
  fi
  fixture_digest=${digest}
  docker exec "${node}" /usr/local/bin/ctr -n k8s.io images tag "${fixture_image_name}" "docker.io/library/kube-ovn-central-fixture@${digest}" >/dev/null
done
member_image="docker.io/library/kube-ovn-central-fixture@${fixture_digest}"

# ovsdb-tool create-cluster generates the cluster UUID. Generate one shared
# NB/SB cluster outside the Pods, then create a distinct native member file for
# each control-plane address. This avoids making later Pods guess the first
# member's random cluster UUID and keeps the fixture independent of Kubernetes
# API credentials.
seed_dir="${test_dir}/controller-seed"
mkdir -m 0700 "${seed_dir}"
control_plane_nodes=()
control_plane_ips=()
while read -r ip node; do
  control_plane_ips+=("${ip}")
  control_plane_nodes+=("${node}")
done < <(
  for node in $(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    ip=$(kubectl get node "${node}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
    printf '%s %s\n' "${ip}" "${node}"
  done | sort -k1,1
)
if [[ "${#control_plane_ips[@]}" -ne 3 ]]; then
  echo "expected three control-plane nodes, got ${#control_plane_ips[@]}" >&2
  exit 1
fi
master_nodes=$(IFS=,; printf '%s' "${control_plane_ips[*]}")
docker run --rm --network=none \
  --user 0:0 \
  --env "MASTER_NODES=${master_nodes}" \
  --volume "${seed_dir}:/controller-seed" \
  --entrypoint /bin/sh kube-ovn-central-fixture:e2e -ec '
    set -eu
    tool=/opt/ovn-native/ovsdb-tool
    schema_dir=/usr/share/ovn
    old_ifs=${IFS}
    IFS=,
    nodes="${MASTER_NODES}"
    IFS=${old_ifs}
    first_node=${MASTER_NODES%%,*}
    "${tool}" create-cluster /controller-seed/ovnnb-0.db "${schema_dir}/ovn-nb.ovsschema" "tcp:${first_node}:6643" >/dev/null
    "${tool}" create-cluster /controller-seed/ovnsb-0.db "${schema_dir}/ovn-sb.ovsschema" "tcp:${first_node}:6644" >/dev/null
    nb_cid=$("${tool}" db-cid /controller-seed/ovnnb-0.db)
    sb_cid=$("${tool}" db-cid /controller-seed/ovnsb-0.db)
    index=1
    for node in ${nodes}; do
      if [ "${node}" = "${first_node}" ]; then
        continue
      fi
      "${tool}" --cid="${nb_cid}" join-cluster "/controller-seed/ovnnb-${index}.db" OVN_Northbound "tcp:${node}:6643" "tcp:${first_node}:6643"
      "${tool}" --cid="${sb_cid}" join-cluster "/controller-seed/ovnsb-${index}.db" OVN_Southbound "tcp:${node}:6644" "tcp:${first_node}:6644"
      index=$((index + 1))
    done
    chmod 0644 /controller-seed/*.db
  '
for index in 0 1 2; do
  node=${control_plane_nodes[${index}]}
  docker exec "${node}" mkdir -p /tmp/kube-ovn-operator-controller-fixture
  # Copy into the node's existing /tmp first. Docker's cp implementation can
  # reject a nested destination even after docker exec created its parent.
  docker cp "${seed_dir}/ovnnb-${index}.db" "${node}:/tmp/"
  docker cp "${seed_dir}/ovnsb-${index}.db" "${node}:/tmp/"
  docker exec "${node}" sh -ec "
    mkdir -p /tmp/kube-ovn-operator-controller-fixture
    mv /tmp/ovnnb-${index}.db /tmp/kube-ovn-operator-controller-fixture/ovnnb.db
    mv /tmp/ovnsb-${index}.db /tmp/kube-ovn-operator-controller-fixture/ovnsb.db
  "
done

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
# The provisioner deliberately refuses to chmod/chown existing storage. Create
# the private root as the workload UID before the hostPath PV is bound.
docker exec --user 65532:65532 "${worker}" mkdir -m 0700 /tmp/kube-ovn-operator-controller-replacement
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

journal_phase() {
  kubectl get configmap "$1" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}' |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["phase"])'
}

final_phase=""
for _ in $(seq 1 240); do
  final_phase=$(journal_phase "$journal_name" 2>/dev/null || true)
  if [[ "$final_phase" == Complete ]]; then
    break
  fi
  sleep 2
done
if [[ "$final_phase" != Complete ]]; then
  echo "lifecycle did not reach Complete (last phase: $final_phase)" >&2
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  kubectl get jobs,statefulsets,pods,configmaps -n kube-ovn-operator-system -o wide || true
  cat "$test_dir/operator.log"
  exit 1
fi

complete_name="$request_name-complete"
kubectl get configmap "$complete_name" -n kube-ovn-operator-system >/dev/null
complete_json=$(kubectl get configmap "$complete_name" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "$complete_json" | grep -Fq '"phase":"Complete"'
printf '%s\n' "$complete_json" | grep -Fq "$request_name"

# The completion receipt and journal must retain the same immutable request,
# while the old Deployment has exactly two survivors and the new member is an
# independently owned, ready StatefulSet.
final_replicas=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "$final_replicas" != 2 ]]; then
  echo "expected the retained Chart Deployment to converge to two replicas, got $final_replicas" >&2
  exit 1
fi
if kubectl get pod "$target_pod" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo "retired Deployment Pod $target_pod still exists" >&2
  exit 1
fi

replacement_set=""
replacement_pod=""
replacement_uid=""
for _ in $(seq 1 120); do
  replacement_set=$(kubectl get statefulset -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record' -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  replacement_pod=$(kubectl get pod -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record,app=ovn-central' -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "$replacement_set" && -n "$replacement_pod" ]]; then
    ready=$(kubectl get statefulset "$replacement_set" -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    replacement_uid=$(kubectl get pod "$replacement_pod" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    if [[ "$ready" == 1 && -n "$replacement_uid" ]]; then
      break
    fi
  fi
  sleep 2
done
if [[ -z "$replacement_set" || -z "$replacement_pod" || -z "$replacement_uid" ]]; then
  echo 'replacement StatefulSet or Pod did not become ready' >&2
  kubectl get statefulsets,pods -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record' -o yaml || true
  exit 1
fi
sleep 5
if [[ "$(kubectl get pod "$replacement_pod" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')" != "$replacement_uid" ]]; then
  echo 'replacement Pod identity changed after lifecycle completion' >&2
  exit 1
fi

operation_handoff=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.handoffApplied}')
if [[ "$operation_handoff" != true ]]; then
  echo "lifecycle apply checkpoint was not consumed: $operation_handoff" >&2
  exit 1
fi

final_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
if [[ -z "$final_inventory" ]]; then
  echo 'final topology inventory was not retained' >&2
  exit 1
fi
topology_json=$(kubectl get configmap "$final_inventory" -n kube-ovn-operator-system -o jsonpath='{.data.topology\.json}')
TOPOLOGY_JSON="$topology_json" TARGET_POD="$target_pod" REPLACEMENT_POD="$replacement_pod" python3 - <<'PYTHON'
import json
import os

topology = json.loads(os.environ["TOPOLOGY_JSON"])["topology"]["members"]
if len(topology) != 3:
    raise SystemExit(f"expected three final central members, got {len(topology)}")
if any(member["pod"]["name"] == os.environ["TARGET_POD"] for member in topology):
    raise SystemExit("retired Deployment Pod remains in final topology")
replacement = [member for member in topology if member["pod"]["name"] == os.environ["REPLACEMENT_POD"]]
if len(replacement) != 1 or replacement[0]["workload"]["kind"] != "StatefulSet":
    raise SystemExit("final topology does not contain exactly one replacement StatefulSet member")
if sum(member["workload"]["kind"] == "Deployment" for member in topology) != 2:
    raise SystemExit("final topology does not retain exactly two Chart Deployment members")
for member in topology:
    if any(not database["serverID"] for database in member["databases"]):
        raise SystemExit("final topology contains an incomplete NB/SB identity")
PYTHON

# Request and journal references remain durable after the controller has
# consumed the handoff authorization.
final_request_json=$(kubectl get configmap "$request_name" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
if [[ "$final_request_json" != "$request_json" ]]; then
  echo 'immutable lifecycle request changed after completion' >&2
  exit 1
fi
journal_json=$(kubectl get configmap "$journal_name" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "$journal_json" | grep -Fq '"phase":"Complete"'
printf '%s\n' "$journal_json" | grep -Fq "$replacement_pod"
echo "default controller completed native Deployment replacement: $target_pod -> $replacement_pod; StatefulSet=$replacement_set; inventory=$final_inventory"
