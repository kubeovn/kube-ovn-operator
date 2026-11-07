#!/usr/bin/env bash
set -euo pipefail

cluster_name=kube-ovn-operator-network
network_stack=${NETWORK_STACK:-ipv4}
service_family_policy=SingleStack
case "${network_stack}" in
  ipv4) expected_families=IPv4 ;;
  dual_stack) expected_families=IPv4,IPv6; service_family_policy=RequireDualStack ;;
  *) echo "unsupported network acceptance stack: ${network_stack}" >&2; exit 1 ;;
esac
log_dir=/tmp/kube-ovn-operator-network-logs
mkdir -p "${log_dir}"
operator_binary=$(mktemp)
cleanup() {
  kubectl get nodes -o wide >"${log_dir}/nodes.txt" 2>&1 || true
  kubectl -n kube-system get pods,deployments,daemonsets,services -o wide >"${log_dir}/workloads.txt" 2>&1 || true
  kubectl get events -A --sort-by=.lastTimestamp >"${log_dir}/events.txt" 2>&1 || true
  kubectl -n kube-system get kubeovninstallation/network -o yaml >"${log_dir}/installation.yaml" 2>&1 || true
  kubectl -n default get pods,services -o wide >"${log_dir}/independent-probes.txt" 2>&1 || true
  if [[ -n "${operator_pid:-}" ]]; then
    kill "${operator_pid}" 2>/dev/null || true
    wait "${operator_pid}" 2>/dev/null || true
  fi
  rm -f "${operator_binary}"
}
trap cleanup EXIT

# These settings are dedicated to this disposable kind cluster. The operator
# uses the job's existing kubeconfig; production ServiceAccount RBAC is a
# separate deployment concern.
kubectl config use-context "kind-${cluster_name}"
bash test/e2e/probe-image.sh "${cluster_name}"
for image in docker.io/kubeovn/kube-ovn:v1.16.13 registry.k8s.io/e2e-test-images/agnhost:2.53 docker.io/curlimages/curl:8.14.1; do
  docker pull --platform=linux/amd64 "${image}"
  image_archive=$(mktemp)
  # kind's Docker-image loader imports every manifest platform. Some upstream
  # multi-platform images carry attestations whose child blobs are not present
  # in the runner's local store, so export the exact node platform first.
  docker image save --platform=linux/amd64 --output "${image_archive}" "${image}"
  kind load image-archive --name "${cluster_name}" "${image_archive}"
  rm -f "${image_archive}"
done
kubectl apply -f config/crd/bases
go build -o "${operator_binary}" ./cmd/operator
"${operator_binary}" --leader-elect=false --metrics-bind-address=0 --health-probe-bind-address=:18084 >"${log_dir}/operator.log" 2>&1 &
operator_pid=$!
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18084/readyz >/dev/null 2>&1; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18084/readyz >/dev/null
kubectl apply -f - <<YAML
apiVersion: operator.kubeovn.io/v1alpha1
kind: KubeOVNInstallation
metadata:
  name: network
  namespace: kube-system
spec:
  version: v1.16.13
  chart:
    source: https://github.com/kubeovn/kube-ovn/releases/download/v1.16.13/kube-ovn-v1.16.13.tgz
    digest: sha256:24df922c7aa78e4a21e77289a6eec9a19ac50cb00953df9ada9291b61a638475
  mode: full
  networkStack: ${network_stack}
  central:
    mode: raft
    replicas: 3
  dataPlane:
    ovsUpdateStrategy: RollingUpdate
  networkProbe:
    image: kube-ovn-operator-probe:e2e
YAML
kubectl -n kube-system wait koi/network --for=condition=WorkloadsReady --timeout=900s
kubectl -n kube-system wait koi/network --for=condition=OVNDBReady --timeout=180s
kubectl -n kube-system wait koi/network --for=condition=NorthdReady --timeout=180s
kubectl -n kube-system wait koi/network --for=condition=DataPlaneReady --timeout=180s
kubectl -n kube-system wait koi/network --for=condition=NetworkReady --timeout=180s
kubectl -n kube-system wait koi/network --for=condition=Ready --timeout=180s
kubectl wait nodes --all --for=condition=Ready --timeout=180s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s

# Record actual NB/SB cluster state independently of NODE_IPS.
mapfile -t central_pods < <(kubectl -n kube-system get pods -l app=ovn-central -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
[[ "${#central_pods[@]}" == 3 ]]
for pod in "${central_pods[@]}"; do
  kubectl -n kube-system exec "${pod}" -- ovs-appctl -t /var/run/ovn/ovnnb_db.ctl cluster/status OVN_Northbound >"${log_dir}/${pod}-nb.txt"
  kubectl -n kube-system exec "${pod}" -- ovs-appctl -t /var/run/ovn/ovnsb_db.ctl cluster/status OVN_Southbound >"${log_dir}/${pod}-sb.txt"
done
mapfile -t nodes < <(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
[[ "${#nodes[@]}" == 3 ]]
kubectl apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: operator-network-server
  namespace: default
  labels:
    app: operator-network-server
spec:
  nodeName: ${nodes[0]}
  containers:
    - name: server
      image: registry.k8s.io/e2e-test-images/agnhost:2.53
      args: [netexec, --http-port=8080]
      readinessProbe:
        httpGet:
          path: /hostname
          port: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: operator-network-server
  namespace: default
spec:
  ipFamilyPolicy: ${service_family_policy}
  selector:
    app: operator-network-server
  ports:
    - port: 8080
      targetPort: 8080
---
apiVersion: v1
kind: Pod
metadata:
  name: operator-network-client
  namespace: default
spec:
  nodeName: ${nodes[1]}
  containers:
    - name: client
      image: docker.io/curlimages/curl:8.14.1
      command: [sleep, '3600']
YAML
kubectl wait -n default pod/operator-network-server pod/operator-network-client --for=condition=Ready --timeout=180s
kubectl -n default get pod/operator-network-server -o json >"${log_dir}/server.json"
kubectl -n default get pod/operator-network-client -o json >"${log_dir}/client.json"
kubectl -n default get service/operator-network-server -o json >"${log_dir}/service.json"
kubectl -n kube-system get koi/network -o json >"${log_dir}/network-evidence.json"
python3 - "${log_dir}" "${expected_families}" "${network_stack}" <<'PYTHON' >"${log_dir}/destinations.tsv"
import ipaddress
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
expected = set(sys.argv[2].split(","))
def addresses(values):
    result = {}
    for value in values:
        family = f"IPv{ipaddress.ip_address(value).version}"
        if family in result:
            raise SystemExit(f"duplicate {family} address")
        result[family] = value
    if set(result) != expected:
        raise SystemExit(f"address families {set(result)} differ from requested {expected}")
    return result

server = json.loads((directory / "server.json").read_text())
client = json.loads((directory / "client.json").read_text())
service = json.loads((directory / "service.json").read_text())
installation = json.loads((directory / "network-evidence.json").read_text())
if installation["spec"].get("networkStack") != sys.argv[3]:
    raise SystemExit("API server did not retain the requested networkStack")
evidence = installation["status"]["network"]
if set(evidence["addressFamilies"]) != expected or evidence["observedNodes"] != 3 or evidence["successfulProbes"] != 12 * len(expected):
    raise SystemExit(f"incomplete controller network evidence: {evidence}")
if server["spec"]["nodeName"] == client["spec"]["nodeName"]:
    raise SystemExit("independent network probes must cross nodes")
addresses([entry["ip"] for entry in client["status"]["podIPs"]])
for kind, values in [("Pod", [entry["ip"] for entry in server["status"]["podIPs"]]), ("Service", service["spec"]["clusterIPs"])]:
    for family, value in sorted(addresses(values).items()):
        host = f"[{value}]" if family == "IPv6" else value
        print(f"{family}\t{kind}\t{host}")
for family in sorted(expected):
    print(f"{family}\tDNS\toperator-network-server.default.svc.cluster.local")
PYTHON
while IFS=$'\t' read -r family kind destination; do
  family_flag=--ipv4
  if [[ "${family}" == IPv6 ]]; then family_flag=--ipv6; fi
  response=$(kubectl -n default exec operator-network-client -- curl "${family_flag}" --noproxy '*' \
    --fail --silent --show-error --max-time 15 "http://${destination}:8080/hostname")
  [[ "${response}" == operator-network-server ]]
  printf '%s %s connectivity passed\n' "${family}" "${kind}" | tee -a "${log_dir}/connectivity.txt"
done <"${log_dir}/destinations.tsv"
# Independent probes supplement the controller's network verification.
kubectl -n kube-system get koi/network -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' | grep -qx VerificationComplete
