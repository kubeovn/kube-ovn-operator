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
    first_node=${MASTER_NODES%%,*}
    "${tool}" create-cluster /controller-seed/ovnnb-0.db "${schema_dir}/ovn-nb.ovsschema" "tcp:${first_node}:6643" >/dev/null
    "${tool}" create-cluster /controller-seed/ovnsb-0.db "${schema_dir}/ovn-sb.ovsschema" "tcp:${first_node}:6644" >/dev/null
    nb_cid=$("${tool}" db-cid /controller-seed/ovnnb-0.db)
    sb_cid=$("${tool}" db-cid /controller-seed/ovnsb-0.db)
    index=1
    old_ifs=${IFS}
    IFS=,
    for node in ${MASTER_NODES}; do
      if [ "${node}" = "${first_node}" ]; then
        continue
      fi
      "${tool}" --cid="${nb_cid}" join-cluster "/controller-seed/ovnnb-${index}.db" OVN_Northbound "tcp:${node}:6643" "tcp:${first_node}:6643"
      "${tool}" --cid="${sb_cid}" join-cluster "/controller-seed/ovnsb-${index}.db" OVN_Southbound "tcp:${node}:6644" "tcp:${first_node}:6644"
      index=$((index + 1))
    done
    IFS=${old_ifs}
    if [ "${index}" -ne 3 ]; then
      echo "expected three seeded members, got ${index}" >&2
      exit 1
    fi
    chmod 0644 /controller-seed/*.db
  '
for index in 0 1 2; do
  node=${control_plane_nodes[${index}]}
  # Stream the native files through docker exec. On the hosted kind runner,
  # docker cp can report success without leaving a copied file in the node
  # filesystem, so avoid its daemon-side path handling entirely.
  docker exec -i "${node}" sh -ec "
    mkdir -p /tmp/kube-ovn-operator-controller-fixture
    cat > /tmp/kube-ovn-operator-controller-fixture/ovnnb.db
    chmod 0644 /tmp/kube-ovn-operator-controller-fixture/ovnnb.db
  " < "${seed_dir}/ovnnb-${index}.db"
  docker exec -i "${node}" sh -ec "
    cat > /tmp/kube-ovn-operator-controller-fixture/ovnsb.db
    chmod 0644 /tmp/kube-ovn-operator-controller-fixture/ovnsb.db
  " < "${seed_dir}/ovnsb-${index}.db"
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
echo "lifecycle completion receipt verified: ${complete_name}"

# The completion receipt and journal must retain the same immutable request,
# while the old Deployment has exactly two survivors and the new member is an
# independently owned, ready StatefulSet.
final_replicas=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "$final_replicas" != 2 ]]; then
  echo "expected the retained Chart Deployment to converge to two replicas, got $final_replicas" >&2
  exit 1
fi
echo "retained Chart Deployment scaled to two replicas"
if kubectl get pod "$target_pod" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo "retired Deployment Pod $target_pod still exists" >&2
  exit 1
fi
echo "retired Deployment Pod removed: ${target_pod}"

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
echo "replacement StatefulSet ready: ${replacement_set}/${replacement_pod}"
sleep 5
if [[ "$(kubectl get pod "$replacement_pod" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')" != "$replacement_uid" ]]; then
  echo 'replacement Pod identity changed after lifecycle completion' >&2
  exit 1
fi

operation_handoff=""
for _ in $(seq 1 30); do
  operation_handoff=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.handoffApplied}' 2>/dev/null || true)
  if [[ "$operation_handoff" == true ]]; then
    break
  fi
  sleep 2
done
if [[ "$operation_handoff" != true ]]; then
	kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory}{"\n"}' >&2 || true
	kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.handoffInventory}{"\n"}' >&2 || true
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation}' >&2 || true
  printf '\n' >&2
	cat "${test_dir}/operator.log" >&2 || true
  echo "lifecycle apply checkpoint was not consumed: $operation_handoff" >&2
  exit 1
fi
echo "lifecycle handoff authorization consumed"

final_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
if [[ -z "$final_inventory" ]]; then
  echo 'final topology inventory was not retained' >&2
  exit 1
fi
echo "final topology inventory retained: ${final_inventory}"
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
echo "final topology identity checks passed"

# Request and journal references remain durable after the controller has
# consumed the handoff authorization.
final_request_json=$(kubectl get configmap "$request_name" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
if [[ "$final_request_json" != "$request_json" ]]; then
  echo 'immutable lifecycle request changed after completion' >&2
  exit 1
fi
journal_json=$(kubectl get configmap "$journal_name" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "$journal_json" | grep -Fq '"phase":"Complete"'
printf '%s\n' "$journal_json" | grep -Fq "$replacement_set"
echo "immutable request and completion journal verified"
echo "default controller completed native Deployment replacement: $target_pod -> $replacement_pod; StatefulSet=$replacement_set; inventory=$final_inventory"

# Exercise automatic loss inference after the explicit replacement. The
# approval is created before the endpoint is lost, and the replacement request
# deliberately omits replaceEndpoint. Cordon plus Pod deletion makes the old
# endpoint uniquely lost while preserving the healthy Deployment survivor.
automatic_target_pod=$(kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o json | python3 -c '
import json
import sys

for item in json.load(sys.stdin).get("items", []):
    owners = item.get("metadata", {}).get("ownerReferences", [])
    if any(owner.get("controller") and owner.get("kind") == "ReplicaSet" for owner in owners):
        print(item["metadata"]["name"])
        break
')
if [[ -z "${automatic_target_pod}" ]]; then
  echo 'could not identify a Deployment Pod for automatic loss replacement' >&2
  exit 1
fi
automatic_target_node=$(kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system -o jsonpath='{.spec.nodeName}')
if [[ -z "${automatic_target_node}" ]]; then
  echo "Deployment Pod ${automatic_target_pod} has no target Node" >&2
  exit 1
fi

automatic_claim_name=controller-auto-replacement-data
automatic_volume_name=controller-auto-replacement-volume
automatic_storage_path=/tmp/kube-ovn-operator-controller-auto-replacement
docker exec --user 65532:65532 "${worker}" mkdir -m 0700 "${automatic_storage_path}"
automatic_claim_uid=""
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${automatic_claim_name}
  namespace: kube-ovn-operator-system
spec:
  storageClassName: ""
  volumeName: ${automatic_volume_name}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
YAML
for _ in $(seq 1 60); do
  automatic_claim_uid=$(kubectl get pvc "${automatic_claim_name}" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [[ -n "${automatic_claim_uid}" ]]; then
    break
  fi
  sleep 1
done
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${automatic_volume_name}
spec:
  storageClassName: ""
  capacity:
    storage: 1Gi
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  claimRef:
    name: ${automatic_claim_name}
    namespace: kube-ovn-operator-system
    uid: ${automatic_claim_uid}
  hostPath:
    path: ${automatic_storage_path}
    type: DirectoryOrCreate
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: [${worker}]
YAML
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/${automatic_claim_name} -n kube-ovn-operator-system --timeout=60s >/dev/null

automatic_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
automatic_topology=$(kubectl get configmap "${automatic_inventory}" -n kube-ovn-operator-system -o jsonpath='{.data.topology\.json}')
installation_uid=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')
automatic_approval_name=automatic-lost-approval
automatic_approval_operation_id=eeee0000-1111-2222-3333-444444444444
TOPOLOGY_JSON="${automatic_topology}" TARGET_POD="${automatic_target_pod}" INSTALLATION_UID="${installation_uid}" APPROVAL_NAME="${automatic_approval_name}" OPERATION_ID="${automatic_approval_operation_id}" python3 - <<'PYTHON' >"${test_dir}/automatic-approval.json"
import datetime
import json
import os

topology = json.loads(os.environ["TOPOLOGY_JSON"])["topology"]["members"]
target = next((member for member in topology if member["pod"]["name"] == os.environ["TARGET_POD"]), None)
if target is None:
    raise SystemExit(f"target Pod {os.environ['TARGET_POD']} is absent from retained topology")
targets = [
    {
        "database": database["database"],
        "clusterID": database["clusterID"],
        "serverID": database["serverID"],
        "address": database["address"],
    }
    for database in target["databases"]
]
if len(targets) != 2:
    raise SystemExit("automatic loss approval requires both NB and SB targets")
expires = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).isoformat().replace("+00:00", "Z")
approval = {
    "apiVersion": "operator.kubeovn.io/v1alpha1",
    "kind": "KubeOVNMemberRemovalApproval",
    "metadata": {"name": os.environ["APPROVAL_NAME"], "namespace": "kube-ovn-operator-system"},
    "spec": {
        "decision": {
            "installationRef": {"name": "controller-retirement", "uid": os.environ["INSTALLATION_UID"]},
            "operationID": os.environ["OPERATION_ID"],
            "targets": targets,
            "reason": "kind smoke test for approved automatic endpoint loss replacement",
            "expiresAt": expires,
        }
    },
}
print(json.dumps(approval))
PYTHON
kubectl apply -f "${test_dir}/automatic-approval.json" >/dev/null
automatic_approval_uid=$(kubectl get kubeovnmemberremovalapproval "${automatic_approval_name}" -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')
if [[ -z "${automatic_approval_uid}" ]]; then
  echo 'automatic loss approval has no UID' >&2
  exit 1
fi

cat >"${test_dir}/automatic-replacement.json" <<JSON
{"spec":{"central":{"memberRequests":[{"name":"automatic-replacement","nodeName":"${worker}","claimName":"${automatic_claim_name}","volumeName":"${automatic_volume_name}","image":"${member_image}","northboundAddress":"tcp:${worker_ip}:6643","southboundAddress":"tcp:${worker_ip}:6644","northboundClientAddress":"tcp:${worker_ip}:6641","southboundClientAddress":"tcp:${worker_ip}:6642","removalApprovalRef":{"name":"${automatic_approval_name}","uid":"${automatic_approval_uid}"}}]}}}
JSON
kubectl patch koi/controller-retirement -n kube-ovn-operator-system --type=merge --patch-file "${test_dir}/automatic-replacement.json" >/dev/null
kubectl cordon "${automatic_target_node}" >/dev/null
kubectl delete pod "${automatic_target_pod}" -n kube-ovn-operator-system --wait=false >/dev/null

previous_request_name=${request_name}
automatic_request_name=""
for _ in $(seq 1 120); do
  automatic_request_name=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.requestName}' 2>/dev/null || true)
  if [[ -n "${automatic_request_name}" && "${automatic_request_name}" != "${previous_request_name}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${automatic_request_name}" || "${automatic_request_name}" == "${previous_request_name}" ]]; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
  exit 1
fi
automatic_request_json=$(kubectl get configmap "${automatic_request_name}" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "${automatic_request_json}" | grep -Fq "${automatic_target_pod}"
printf '%s\n' "${automatic_request_json}" | grep -Fq '"automaticReplacement":true'
printf '%s\n' "${automatic_request_json}" | grep -Fq '"endpointMissing":true'
printf '%s\n' "${automatic_request_json}" | grep -Fq "${automatic_approval_name}"
printf '%s\n' "${automatic_request_json}" | grep -Fq "${automatic_approval_operation_id}"
automatic_journal_name=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.journalName}')
echo "automatic loss replacement admitted: ${automatic_target_pod} on ${automatic_target_node}; request=${automatic_request_name}"

automatic_phase=""
for _ in $(seq 1 240); do
  automatic_phase=$(journal_phase "${automatic_journal_name}" 2>/dev/null || true)
  if [[ "${automatic_phase}" == Complete ]]; then
    break
  fi
  sleep 2
done
if [[ "${automatic_phase}" != Complete ]]; then
  echo "automatic loss replacement did not reach Complete (last phase: ${automatic_phase})" >&2
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  kubectl get deployments,statefulsets,pods,configmaps -n kube-ovn-operator-system -o wide || true
  cat "${test_dir}/operator.log"
  exit 1
fi
automatic_replicas=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "${automatic_replicas}" != 1 ]]; then
  echo "expected Chart Deployment to scale to one replica after automatic replacement, got ${automatic_replicas}" >&2
  exit 1
fi
if kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo "automatically retired Deployment Pod ${automatic_target_pod} still exists" >&2
  exit 1
fi
automatic_set=""
automatic_pod=""
for _ in $(seq 1 120); do
  automatic_set=$(kubectl get statefulsets -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record' -o json | OLD_SET="${replacement_set}" python3 -c '
import json
import os
import sys

for item in json.load(sys.stdin).get("items", []):
    if item["metadata"]["name"] != os.environ["OLD_SET"]:
        print(item["metadata"]["name"])
        break
')
  if [[ -n "${automatic_set}" ]]; then
    automatic_pod=$(kubectl get pods -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record,app=ovn-central' -o json | SET_NAME="${automatic_set}" python3 -c '
import json
import os
import sys

for item in json.load(sys.stdin).get("items", []):
    owners = item.get("metadata", {}).get("ownerReferences", [])
    if any(owner.get("controller") and owner.get("kind") == "StatefulSet" and owner.get("name") == os.environ["SET_NAME"] for owner in owners):
        print(item["metadata"]["name"])
        break
')
    ready=$(kubectl get statefulset "${automatic_set}" -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    if [[ "${ready}" == 1 && -n "${automatic_pod}" ]]; then
      break
    fi
  fi
  sleep 2
done
if [[ -z "${automatic_set}" || -z "${automatic_pod}" ]]; then
  echo 'automatic replacement StatefulSet or Pod did not become ready' >&2
  kubectl get statefulsets,pods -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record' -o wide || true
  exit 1
fi
if [[ "$(kubectl get kubeovnmemberremovalapproval "${automatic_approval_name}" -n kube-ovn-operator-system -o jsonpath='{.status.phase}')" != Completed ]]; then
  echo 'automatic loss approval did not reach Completed' >&2
  kubectl get kubeovnmemberremovalapproval "${automatic_approval_name}" -n kube-ovn-operator-system -o yaml || true
  exit 1
fi
automatic_final_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
automatic_final_topology=$(kubectl get configmap "${automatic_final_inventory}" -n kube-ovn-operator-system -o jsonpath='{.data.topology\.json}')
AUTO_TOPOLOGY="${automatic_final_topology}" TARGET_POD="${automatic_target_pod}" AUTOMATIC_POD="${automatic_pod}" python3 - <<'PYTHON'
import json
import os

members = json.loads(os.environ["AUTO_TOPOLOGY"])["topology"]["members"]
if len(members) != 3:
    raise SystemExit(f"expected three members after automatic replacement, got {len(members)}")
if any(member["pod"]["name"] == os.environ["TARGET_POD"] for member in members):
    raise SystemExit("automatically retired Deployment Pod remains in the topology")
replacement = [member for member in members if member["pod"]["name"] == os.environ["AUTOMATIC_POD"]]
if len(replacement) != 1 or replacement[0]["workload"]["kind"] != "StatefulSet":
    raise SystemExit("automatic replacement StatefulSet is missing from the topology")
if sum(member["workload"]["kind"] == "Deployment" for member in members) != 1:
    raise SystemExit("automatic replacement did not leave one Chart Deployment member")
if sum(member["workload"]["kind"] == "StatefulSet" for member in members) != 2:
    raise SystemExit("automatic replacement did not retain two StatefulSet members")
PYTHON
echo "automatic Deployment replacement completed: ${automatic_target_pod} retired and Chart Deployment scaled to one"
echo "automatic replacement StatefulSet ready: ${automatic_set}/${automatic_pod}"
request_name=${automatic_request_name}

# Exercise a second lifecycle retirement after automatic replacement. The
# automatic replacement leaves one Chart Deployment member and two
# independently managed StatefulSet members. Reducing the desired count to two
# must remove exactly one healthy StatefulSet member while preserving the Chart
# Deployment and the other replacement.
scale_in_target=$(kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o json | python3 -c '
import json
import sys

for item in json.load(sys.stdin).get("items", []):
    owners = item.get("metadata", {}).get("ownerReferences", [])
    if any(owner.get("controller") and owner.get("kind") == "StatefulSet" for owner in owners):
        print(item["metadata"]["name"])
        break
')
if [[ -z "${scale_in_target}" ]]; then
  echo 'could not identify an independent StatefulSet Pod for scale-in' >&2
  exit 1
fi
cat >"${test_dir}/scale-in.json" <<JSON
{"spec":{"central":{"replicas":2,"memberRequests":[{"name":"retire-chart","retireEndpoint":"${scale_in_target}"}]}}}
JSON
previous_request_name=${request_name}
kubectl patch koi/controller-retirement -n kube-ovn-operator-system --type=merge --patch-file "${test_dir}/scale-in.json" >/dev/null

scale_in_request=""
for _ in $(seq 1 90); do
  scale_in_request=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.requestName}' 2>/dev/null || true)
  if [[ -n "${scale_in_request}" && "${scale_in_request}" != "${previous_request_name}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${scale_in_request}" || "${scale_in_request}" == "${previous_request_name}" ]]; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
  exit 1
fi
scale_in_journal=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.journalName}')
scale_in_request_json=$(kubectl get configmap "${scale_in_request}" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
printf '%s\n' "${scale_in_request_json}" | grep -Fq "${scale_in_target}"
printf '%s\n' "${scale_in_request_json}" | grep -Fq '"kind":"StatefulSet"'

scale_in_phase=""
for _ in $(seq 1 240); do
  scale_in_phase=$(journal_phase "${scale_in_journal}" 2>/dev/null || true)
  if [[ "${scale_in_phase}" == Complete ]]; then
    break
  fi
  sleep 2
done
if [[ "${scale_in_phase}" != Complete ]]; then
  echo "StatefulSet retirement did not reach Complete (last phase: ${scale_in_phase})" >&2
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  kubectl get deployments,statefulsets,pods,configmaps -n kube-ovn-operator-system -o wide || true
  cat "${test_dir}/operator.log"
  exit 1
fi
scale_in_replicas=$(kubectl get deployment/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "${scale_in_replicas}" != 1 ]]; then
  echo "expected Chart Deployment to remain at one replica after StatefulSet retirement, got ${scale_in_replicas}" >&2
  exit 1
fi
if kubectl get pod "${scale_in_target}" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo "retired StatefulSet Pod ${scale_in_target} still exists after scale-in" >&2
  exit 1
fi
echo "StatefulSet retirement completed: ${scale_in_target} retired after joint convergence"

scale_in_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
scale_in_topology=$(kubectl get configmap "${scale_in_inventory}" -n kube-ovn-operator-system -o jsonpath='{.data.topology\.json}')
SCALE_IN_TOPOLOGY="${scale_in_topology}" RETIRED_POD="${scale_in_target}" python3 - <<'PYTHON'
import json
import os

members = json.loads(os.environ["SCALE_IN_TOPOLOGY"])["topology"]["members"]
if len(members) != 2:
    raise SystemExit(f"expected two final central members after scale-in, got {len(members)}")
if any(member["pod"]["name"] == os.environ["RETIRED_POD"] for member in members):
    raise SystemExit("retired Chart Deployment Pod remains in the post-scale-in topology")
if sum(member["workload"]["kind"] == "Deployment" for member in members) != 1:
    raise SystemExit("post-scale-in topology does not retain exactly one Chart Deployment member")
if sum(member["workload"]["kind"] == "StatefulSet" for member in members) != 1:
    raise SystemExit("post-scale-in topology does not retain the independent replacement member")
for member in members:
    if any(not database["serverID"] for database in member["databases"]):
        raise SystemExit("post-scale-in topology contains an incomplete NB/SB identity")
PYTHON
echo "post-scale-in topology identity checks passed"
