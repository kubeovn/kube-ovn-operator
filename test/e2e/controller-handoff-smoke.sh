#!/usr/bin/env bash
set -euo pipefail

cluster_name="kube-ovn-operator-controller-e2e"
central_deployment="ovn-central"
test_dir=$(mktemp -d)
created_cluster=0
operator_pid=""
server_pid=""
deployment_guard_pid=""
replacement_guard_pid=""

cleanup() {
  if [[ -n "${deployment_guard_pid}" ]]; then
    kill "${deployment_guard_pid}" 2>/dev/null || true
    wait "${deployment_guard_pid}" 2>/dev/null || true
  fi
  if [[ -n "${replacement_guard_pid}" ]]; then
    kill "${replacement_guard_pid}" 2>/dev/null || true
    wait "${replacement_guard_pid}" 2>/dev/null || true
  fi
  if [[ -n "${operator_pid}" ]]; then
    kill -CONT "${operator_pid}" 2>/dev/null || true
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
docker build --target fence-agent -t kube-ovn-fence-agent:e2e . >/dev/null
kind load docker-image --name "${cluster_name}" kube-ovn-central-fixture:e2e
kind load docker-image --name "${cluster_name}" kube-ovn-fence-agent:e2e

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
fence_digest=""
for node in $(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
  digest=$(docker exec "${node}" /usr/local/bin/ctr -n k8s.io images ls | awk '$1 == "docker.io/library/kube-ovn-fence-agent:e2e" {print $3}')
  if [[ ! "${digest}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    echo "could not read fence-agent image digest on ${node}" >&2
    exit 1
  fi
  fence_digest=${digest}
  docker exec "${node}" /usr/local/bin/ctr -n k8s.io images tag "docker.io/library/kube-ovn-fence-agent:e2e" "docker.io/library/kube-ovn-fence-agent@${digest}" >/dev/null
done
fence_image="docker.io/library/kube-ovn-fence-agent@${fence_digest}"

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

# The handoff exercises the same mTLS fence-agent transport used for
# production mutation. These short-lived files and Secrets are scoped to the
# disposable Kind cluster and are removed with it by the trap above.
fence_dir="${test_dir}/fence"
mkdir -m 0700 "${fence_dir}"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj '/CN=kube-ovn-fence-test-ca' \
  -keyout "${fence_dir}/ca.key" -out "${fence_dir}/ca.crt" >/dev/null 2>&1
serial_file="${fence_dir}/ca.srl"
printf '01\n' >"${serial_file}"
san_list=""
# Replacement members run on workers too, so every disposable node address
# must be covered by the test server certificate.
for ip in $(kubectl get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}'); do
  if [[ -n "${san_list}" ]]; then san_list+=,; fi
  san_list+="IP:${ip}"
done
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=kube-ovn-fence-agent' \
  -keyout "${fence_dir}/server.key" -out "${fence_dir}/server.csr" >/dev/null 2>&1
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\n' "${san_list}" >"${fence_dir}/server.ext"
openssl x509 -req -days 1 -CAserial "${serial_file}" \
  -CA "${fence_dir}/ca.crt" -CAkey "${fence_dir}/ca.key" \
  -in "${fence_dir}/server.csr" -out "${fence_dir}/server.crt" \
  -extfile "${fence_dir}/server.ext" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -subj '/CN=kube-ovn-operator-test-client' \
  -keyout "${fence_dir}/client.key" -out "${fence_dir}/client.csr" >/dev/null 2>&1
printf 'extendedKeyUsage=clientAuth\n' >"${fence_dir}/client.ext"
openssl x509 -req -days 1 -CAserial "${serial_file}" \
  -CA "${fence_dir}/ca.crt" -CAkey "${fence_dir}/ca.key" \
  -in "${fence_dir}/client.csr" -out "${fence_dir}/client.crt" \
  -extfile "${fence_dir}/client.ext" >/dev/null 2>&1
kubectl create secret generic fence-server -n kube-ovn-operator-system \
  --from-file=tls.crt="${fence_dir}/server.crt" --from-file=tls.key="${fence_dir}/server.key" >/dev/null
kubectl create secret generic fence-client-ca -n kube-ovn-operator-system \
  --from-file=ca.crt="${fence_dir}/ca.crt" >/dev/null
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
  --fence-endpoint-template='https://{node}:8443' \
  --fence-ca-file="${fence_dir}/ca.crt" \
  --fence-client-cert-file="${fence_dir}/client.crt" \
  --fence-client-key-file="${fence_dir}/client.key" \
  --fence-agent-image="${fence_image}" \
  --fence-agent-server-tls-secret=fence-server \
  --fence-agent-client-ca-secret=fence-client-ca \
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
  ready=$(kubectl get "deployment/${central_deployment}" -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}' 2>/dev/null || true)
  if [[ "${ready}" == 3 && -n "${inventory}" ]]; then
    break
  fi
  sleep 2
done
if [[ "$(kubectl get "deployment/${central_deployment}" -n kube-ovn-operator-system -o jsonpath='{.status.readyReplicas}')" != 3 ]]; then
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

mapfile -t worker_nodes < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
if [[ "${#worker_nodes[@]}" -lt 2 ]]; then
  echo "expected two worker nodes, got ${#worker_nodes[@]}" >&2
  exit 1
fi
worker=${worker_nodes[0]}
automatic_worker=${worker_nodes[1]}
worker_ip=$(kubectl get node "${worker}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
automatic_worker_ip=$(kubectl get node "${automatic_worker}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
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

replicas=$(kubectl get "deployment/${central_deployment}" -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
if [[ "${replicas}" != 3 ]] || ! kubectl get pod "${target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
  echo 'Deployment changed before lifecycle execution was authorized' >&2
  exit 1
fi
echo "default controller admitted Deployment replacement request ${request_name} for ${target_pod} and persisted the journal"

journal_phase() {
  kubectl get configmap "$1" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}' |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["phase"])'
}

# Surface permanent container startup failures before waiting for the full
# membership timeout. Diagnostics contain runtime errors, never Secret data.
check_member_startup() {
  if ! kubectl get pods -n kube-ovn-operator-system -l 'operator.kubeovn.io/member-record,app=ovn-central' -o json | python3 -c '
import json
import sys

failed = False
reasons = {"CreateContainerError", "CreateContainerConfigError", "RunContainerError", "CrashLoopBackOff", "ErrImagePull", "ImagePullBackOff"}
for pod in json.load(sys.stdin).get("items", []):
    for container in pod.get("status", {}).get("initContainerStatuses", []) + pod.get("status", {}).get("containerStatuses", []):
        waiting = container.get("state", {}).get("waiting", {})
        if waiting.get("reason") in reasons:
            print(f"{pod["metadata"]["name"]}/{container["name"]}: {waiting.get("reason")}: {waiting.get("message", "")}", file=sys.stderr)
            failed = True
sys.exit(1 if failed else 0)
'; then
    kubectl get events -n kube-ovn-operator-system --sort-by=.metadata.creationTimestamp >&2 || true
    tail -n 80 "${test_dir}/operator.log" >&2 || true
    return 1
  fi
}

final_phase=""
for _ in $(seq 1 240); do
  final_phase=$(journal_phase "$journal_name" 2>/dev/null || true)
  if [[ "${final_phase}" == Starting ]]; then check_member_startup; fi
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
final_replicas=$(kubectl get "deployment/${central_deployment}" -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
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
# Catch a missing replacement sidecar even when this member is a follower.
curl --fail --silent --show-error --connect-timeout 5 --max-time 10 \
  --cacert "${fence_dir}/ca.crt" --cert "${fence_dir}/client.crt" --key "${fence_dir}/client.key" \
  "https://${worker_ip}:8443/healthz" >/dev/null
echo "replacement fence-agent mTLS endpoint verified"
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
kubectl rollout status deployment/"${central_deployment}" -n kube-ovn-operator-system --timeout=120s
automatic_candidates_file="${test_dir}/automatic-candidates.json"
kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o json | python3 -c '
import json
import sys

items = []
for item in json.load(sys.stdin).get("items", []):
    owners = item.get("metadata", {}).get("ownerReferences", [])
    if any(owner.get("controller") and owner.get("kind") == "ReplicaSet" for owner in owners):
        rs = next(owner["name"] for owner in owners if owner.get("controller") and owner.get("kind") == "ReplicaSet")
        items.append({"name": item["metadata"]["name"], "node": item.get("spec", {}).get("nodeName", ""), "replicaset": rs})
print(json.dumps(items))
' >"${automatic_candidates_file}"
automatic_candidate_count=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))))' "${automatic_candidates_file}")
if [[ "${automatic_candidate_count}" -lt 2 ]]; then
  echo "expected two Deployment Pods before automatic loss, got ${automatic_candidate_count}" >&2
  exit 1
fi

# Stop the local operator while the ReplicaSet is reduced. The explicit
# replacement above may already have retired the initially seeded leader, so
# inspect the live Deployment Pods and choose a follower in both databases.
# The remaining central workload (Deployment or StatefulSet) keeps a leader
# available for the approved kick after the follower disappears.
kill -STOP "${operator_pid}"
automatic_replicaset=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[0]["replicaset"])' "${automatic_candidates_file}")
automatic_roles_file="${test_dir}/automatic-roles.tsv"
automatic_target_pod=""
# Exercise the replacement's authenticated mutation path, not just its TCP
# listener. OVS 3.5's one-shot test hook transfers leadership without changing
# membership and clears itself after one main-loop iteration. Require both
# databases to elect the replacement before admitting the next operation.
for database_socket in ovnnb_db ovnsb_db; do
  database_name=OVN_Northbound
  if [[ "${database_socket}" == ovnsb_db ]]; then database_name=OVN_Southbound; fi
  replacement_is_leader=0
  for _ in $(seq 1 30); do
    replacement_role=$(kubectl exec -n kube-ovn-operator-system "${replacement_pod}" -c ovn-central -- \
      ovs-appctl -t "/var/run/ovn/${database_socket}.ctl" cluster/status "${database_name}" | awk '$1 == "Role:" {print $2}')
    if [[ "${replacement_role}" == leader ]]; then
      replacement_is_leader=1
      break
    fi
    while read -r candidate; do
      candidate_role=$(kubectl exec -n kube-ovn-operator-system "${candidate}" -c ovn-central -- \
        ovs-appctl -t "/var/run/ovn/${database_socket}.ctl" cluster/status "${database_name}" | awk '$1 == "Role:" {print $2}')
      if [[ "${candidate_role}" == leader ]]; then
        kubectl exec -n kube-ovn-operator-system "${candidate}" -c ovn-central -- \
          ovs-appctl -t "/var/run/ovn/${database_socket}.ctl" cluster/failure-test transfer-leadership >/dev/null
      fi
    done < <(python3 -c 'import json,sys; print("\n".join(item["name"] for item in json.load(open(sys.argv[1]))))' "${automatic_candidates_file}")
    sleep 1
  done
  if [[ "${replacement_is_leader}" != 1 ]]; then
    echo "replacement did not become ${database_name} leader before fence-agent acceptance" >&2
    exit 1
  fi
  echo "replacement fence-agent will execute ${database_name} mutations on leader ${replacement_pod}"
done
for _ in $(seq 1 30); do
  : >"${automatic_roles_file}"
  while read -r candidate; do
    role_nb=$(kubectl exec -n kube-ovn-operator-system "${candidate}" -c ovn-central -- \
      ovs-appctl -t /var/run/ovn/ovnnb_db.ctl cluster/status OVN_Northbound 2>/dev/null |
      awk '$1 == "Role:" {print $2}' || true)
    role_sb=$(kubectl exec -n kube-ovn-operator-system "${candidate}" -c ovn-central -- \
      ovs-appctl -t /var/run/ovn/ovnsb_db.ctl cluster/status OVN_Southbound 2>/dev/null |
      awk '$1 == "Role:" {print $2}' || true)
    printf '%s\t%s\t%s\n' "${candidate}" "${role_nb}" "${role_sb}" >>"${automatic_roles_file}"
  done < <(python3 -c 'import json,sys; print("\n".join(item["name"] for item in json.load(open(sys.argv[1]))))' "${automatic_candidates_file}")
  automatic_target_pod=$(python3 -c 'import sys
for line in open(sys.argv[1]):
    name, nb, sb = line.rstrip("\n").split("\t")
    if nb == "follower" and sb == "follower":
        print(name)
        break
' "${automatic_roles_file}")
  if [[ -n "${automatic_target_pod}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${automatic_target_pod}" ]]; then
  kill -CONT "${operator_pid}"
  echo 'could not identify a Deployment follower in both NB and SB databases' >&2
  cat "${automatic_roles_file}" >&2
  exit 1
fi
automatic_target_node=$(CANDIDATES_FILE="${automatic_candidates_file}" TARGET_POD="${automatic_target_pod}" python3 - <<'PYTHON'
import json
import os

for item in json.load(open(os.environ["CANDIDATES_FILE"])):
    if item["name"] == os.environ["TARGET_POD"]:
        print(item["node"])
        break
PYTHON
)
automatic_survivor_pod=$(CANDIDATES_FILE="${automatic_candidates_file}" TARGET_POD="${automatic_target_pod}" python3 - <<'PYTHON'
import json
import os

for item in json.load(open(os.environ["CANDIDATES_FILE"])):
    if item["name"] != os.environ["TARGET_POD"]:
        print(item["name"])
        break
PYTHON
)
if [[ -z "${automatic_survivor_pod}" ]]; then
  kill -CONT "${operator_pid}"
  echo 'could not identify the retained Deployment survivor Pod' >&2
  exit 1
fi
# Pin the actual restarted target to the immutable native identities before
# deleting it. A Pod name is only a process reference, never loss authority.
automatic_target_node_uid=$(kubectl get node "${automatic_target_node}" -o jsonpath='{.metadata.uid}')
for database_socket in ovnnb_db ovnsb_db; do
  database_name=OVN_Northbound
  if [[ "${database_socket}" == ovnsb_db ]]; then database_name=OVN_Southbound; fi
  kubectl exec -n kube-ovn-operator-system "${automatic_target_pod}" -c ovn-central -- \
    ovs-appctl -t "/var/run/ovn/${database_socket}.ctl" cluster/status "${database_name}" \
    >"${test_dir}/automatic-target-${database_name}.txt"
done
# Prevent the ReplicaSet from reopening this same hostPath between deletion
# and approval creation. The healthy retained survivor remains available.
kubectl cordon "${automatic_target_node}" >/dev/null
kubectl annotate pod "${automatic_survivor_pod}" -n kube-ovn-operator-system \
  controller.kubernetes.io/pod-deletion-cost=2147483647 --overwrite >/dev/null
kubectl annotate pod "${automatic_target_pod}" -n kube-ovn-operator-system \
  controller.kubernetes.io/pod-deletion-cost=-2147483648 --overwrite >/dev/null
kubectl patch deployment "${central_deployment}" -n kube-ovn-operator-system --type=merge -p '{"spec":{"paused":true}}' >/dev/null
kubectl scale replicaset "${automatic_replicaset}" -n kube-ovn-operator-system --replicas=1 >/dev/null
for _ in $(seq 1 90); do
  if ! kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
for _ in $(seq 1 30); do
  if ! kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
  kill -CONT "${operator_pid}"
  echo "automatic loss target Pod ${automatic_target_pod} was not removed" >&2
  exit 1
fi

automatic_claim_name=controller-auto-replacement-data
automatic_volume_name=controller-auto-replacement-volume
automatic_storage_path=/tmp/kube-ovn-operator-controller-auto-replacement
docker exec --user 65532:65532 "${automatic_worker}" mkdir -m 0700 "${automatic_storage_path}"
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
              values: [${automatic_worker}]
YAML
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/${automatic_claim_name} -n kube-ovn-operator-system --timeout=60s >/dev/null

automatic_inventory=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.inventory.name}')
automatic_topology=$(kubectl get configmap "${automatic_inventory}" -n kube-ovn-operator-system -o jsonpath='{.data.topology\.json}')
installation_uid=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.metadata.uid}')
automatic_approval_name=automatic-lost-approval
automatic_approval_operation_id=eeee0000-1111-2222-3333-444444444444
TOPOLOGY_JSON="${automatic_topology}" TARGET_NODE="${automatic_target_node}" TARGET_NODE_UID="${automatic_target_node_uid}" TARGET_STATUS_DIR="${test_dir}" INSTALLATION_UID="${installation_uid}" APPROVAL_NAME="${automatic_approval_name}" OPERATION_ID="${automatic_approval_operation_id}" python3 - <<'PYTHON' >"${test_dir}/automatic-approval.json"
import datetime
import json
import os
import pathlib
import re

topology = json.loads(os.environ["TOPOLOGY_JSON"])["topology"]["members"]
candidates = [member for member in topology if member["node"] == {"name": os.environ["TARGET_NODE"], "uid": os.environ["TARGET_NODE_UID"]}]
if len(candidates) != 1:
    raise SystemExit("automatic loss target must match exactly one retained Node identity")
target = candidates[0]
for database in target["databases"]:
    status = (pathlib.Path(os.environ["TARGET_STATUS_DIR"]) / f"automatic-target-{database['database']}.txt").read_text()
    for label, key in (("Cluster ID", "clusterID"), ("Server ID", "serverID")):
        match = re.search(rf"^{label}:.*\(([^)]+)\)", status, re.MULTILINE)
        if match is None or match.group(1) != database[key]:
            raise SystemExit(f"automatic target changed native {key}")
    match = re.search(r"^Address:\s*(\S+)", status, re.MULTILINE)
    if match is None or match.group(1) != database["address"]:
        raise SystemExit("automatic target changed its retained RAFT address")
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
{"spec":{"central":{"memberRequests":[{"name":"automatic-replacement","nodeName":"${automatic_worker}","claimName":"${automatic_claim_name}","volumeName":"${automatic_volume_name}","image":"${member_image}","northboundAddress":"tcp:${automatic_worker_ip}:6643","southboundAddress":"tcp:${automatic_worker_ip}:6644","northboundClientAddress":"tcp:${automatic_worker_ip}:6641","southboundClientAddress":"tcp:${automatic_worker_ip}:6642","removalApprovalRef":{"name":"${automatic_approval_name}","uid":"${automatic_approval_uid}"}}]}}}
JSON
kubectl patch koi/controller-retirement -n kube-ovn-operator-system --type=merge --patch-file "${test_dir}/automatic-replacement.json" >/dev/null

# The retained storage-node constraint leaves the ReplicaSet substitute
# unscheduled after the target node is cordoned. Wait for that exact unstarted
# Pod before admission; it must not reopen data on a retired node. The healthy
# survivor receives the highest deletion cost for the final scale-down.
kubectl annotate pod "${automatic_survivor_pod}" -n kube-ovn-operator-system \
  controller.kubernetes.io/pod-deletion-cost=2147483647 --overwrite >/dev/null
kubectl patch deployment "${central_deployment}" -n kube-ovn-operator-system --type=merge -p '{"spec":{"paused":false}}' >/dev/null
automatic_replacement_pod=""
for _ in $(seq 1 120); do
  automatic_replacement_pod=$(kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o json |
    SURVIVOR_POD="${automatic_survivor_pod}" python3 -c '
import json
import os
import sys

for item in json.load(sys.stdin).get("items", []):
    if item["metadata"]["name"] == os.environ["SURVIVOR_POD"]:
        continue
    owners = item.get("metadata", {}).get("ownerReferences", [])
    if not any(owner.get("controller") and owner.get("kind") == "ReplicaSet" for owner in owners):
        continue
    status = item.get("status", {})
    unstarted = status.get("phase") == "Pending" and not item.get("spec", {}).get("nodeName") and not status.get("containerStatuses") and not status.get("initContainerStatuses")
    if unstarted:
        print(item["metadata"]["name"])
        break
')
  if [[ -n "${automatic_replacement_pod}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${automatic_replacement_pod}" ]]; then
  echo 'Deployment replacement Pod did not remain unstarted before automatic admission' >&2
  kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o wide || true
  exit 1
fi
kill -CONT "${operator_pid}"

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
# Admission has pinned the unique lost endpoint. Keep the Deployment's
# original desired replica count until the durable operation has removed the
# old database member; the lifecycle executor rejects an early scale-down.
automatic_request_json=$(kubectl get configmap "${automatic_request_name}" -n kube-ovn-operator-system -o jsonpath='{.data.record\.json}')
REQUEST_JSON="${automatic_request_json}" APPROVAL_FILE="${test_dir}/automatic-approval.json" python3 - <<'PYTHON'
import json
import os

request = json.loads(os.environ["REQUEST_JSON"])
approval = json.load(open(os.environ["APPROVAL_FILE"]))["spec"]["decision"]
for target in approval["targets"]:
    inventory = next(db for db in request["databases"] if db["database"] == target["database"])
    peer = next(peer for peer in inventory["peers"] if peer["name"] == request["removeEndpoint"])
    if inventory["clusterID"] != target["clusterID"] or any(peer[key] != target[key] for key in ("serverID", "address")):
        raise SystemExit("automatic request does not remove the exact approved native member")
if request["retirement"]["endpoint"]["name"] != request["removeEndpoint"]:
    raise SystemExit("automatic request does not pin the lost process reference")
PYTHON
printf '%s\n' "${automatic_request_json}" | grep -Fq '"automaticReplacement":true'
printf '%s\n' "${automatic_request_json}" | grep -Fq '"endpointMissing":true'
printf '%s\n' "${automatic_request_json}" | grep -Fq "${automatic_approval_name}"
printf '%s\n' "${automatic_request_json}" | grep -Fq "${automatic_approval_operation_id}"
automatic_journal_name=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.journalName}')
echo "automatic loss replacement admitted: ${automatic_target_pod} on ${automatic_target_node}; request=${automatic_request_name}"

automatic_phase=""
for _ in $(seq 1 240); do
  automatic_phase=$(journal_phase "${automatic_journal_name}" 2>/dev/null || true)
  if [[ "${automatic_phase}" == Starting ]]; then check_member_startup; fi
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
# The completed lifecycle still needs one Chart apply to consume its handoff
# authorization. Wait for that reconciliation before reading the final
# inventory; otherwise the status may still point at the pre-operation
# topology. Then immediately replace the one-shot automatic member request
# with the next explicit scale-in request.
kubectl delete pod "${automatic_target_pod}" -n kube-ovn-operator-system --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  if ! kubectl get pod "${automatic_target_pod}" -n kube-ovn-operator-system >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
automatic_replicas=$(kubectl get "deployment/${central_deployment}" -n kube-ovn-operator-system -o jsonpath='{.spec.replicas}')
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
automatic_handoff=""
automatic_operation_request=""
for _ in $(seq 1 90); do
  automatic_operation_request=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.requestName}' 2>/dev/null || true)
  automatic_handoff=$(kubectl get koi/controller-retirement -n kube-ovn-operator-system -o jsonpath='{.status.topology.operation.handoffApplied}' 2>/dev/null || true)
  if [[ "${automatic_operation_request}" == "${automatic_request_name}" && "${automatic_handoff}" == true ]]; then
    break
  fi
  sleep 1
done
if [[ "${automatic_operation_request}" != "${automatic_request_name}" || "${automatic_handoff}" != true ]]; then
  echo 'automatic lifecycle handoff was not consumed before final topology verification' >&2
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  cat "${test_dir}/operator.log"
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
curl --fail --silent --show-error --connect-timeout 5 --max-time 10 \
  --cacert "${fence_dir}/ca.crt" --cert "${fence_dir}/client.crt" --key "${fence_dir}/client.key" \
  "https://${automatic_worker_ip}:8443/healthz" >/dev/null
echo "automatic replacement fence-agent mTLS endpoint verified"

echo "automatic lifecycle handoff authorization consumed"

# Handoff consumption records the accepted apply, not asynchronous rollout
# completion. Bind the recovery fixture only after all retained processes are
# ready on their storage nodes.
kubectl rollout status deployment/"${central_deployment}" -n kube-ovn-operator-system --timeout=120s

# Keep create/list/watch on one API server: the HA load balancer otherwise
# routes kubectl processes to independent, lazily initialized watch caches.
# The existing kubeconfig still supplies its CA and client authentication.
# Exercise the public recovery API while the real controller is still running.
if ! KUBE_OVN_ACCEPTANCE_API_SERVER="https://${control_plane_ips[0]}:6443" python3 test/e2e/fence-takeover-restore.py "${fence_dir}" controller-recovery "${backup_dir}"; then
  kubectl get koi/controller-retirement -n kube-ovn-operator-system -o yaml || true
  kubectl get kubeovnrecoveryrequests -n kube-ovn-operator-system -o yaml || true
  kubectl get deployments,statefulsets -n kube-ovn-operator-system -o yaml || true
  while IFS= read -r pod; do
    for operation in status nb-connection-status sb-connection-status; do
      kubectl exec -n kube-ovn-operator-system "${pod}" -c ovn-central -- ovn-appctl --timeout=5 -t ovn-northd "${operation}" || true
    done
  done < <(kubectl get pods -n kube-ovn-operator-system -l app=ovn-central -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  cat "${test_dir}/operator.log"
  exit 1
fi

# Isolate the agent acceptance from reconciliation and Lease renewal. This
# process is resumed and terminated by cleanup, including on test failure.
kill -STOP "${operator_pid}"
python3 test/e2e/fence-takeover-restore.py "${fence_dir}"
