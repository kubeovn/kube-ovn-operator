#!/usr/bin/env bash
set -euo pipefail

cluster_name=${1:?kind cluster name required}
build_dir=$(mktemp -d)
trap 'rm -rf "${build_dir}"' EXIT
CGO_ENABLED=0 go build -trimpath -o "${build_dir}/network-probe" ./cmd/network-probe
docker build -f test/e2e/probe.Dockerfile -t kube-ovn-operator-probe:e2e "${build_dir}"
kind load docker-image --name "${cluster_name}" kube-ovn-operator-probe:e2e
