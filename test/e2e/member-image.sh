#!/usr/bin/env bash
set -euo pipefail

cluster_name=${1:?kind cluster name required}
member_node="${cluster_name}-control-plane"
docker build --target central-runtime -t kube-ovn-central:e2e .
kind load docker-image --name "${cluster_name}" kube-ovn-central:e2e
member_digest=$(docker exec "${member_node}" ctr -n k8s.io images ls | awk '$1 == "docker.io/library/kube-ovn-central:e2e" {print $3}')
[[ ${member_digest} =~ ^sha256:[a-f0-9]{64}$ ]]
member_image="docker.io/library/kube-ovn-central@${member_digest}"
docker exec "${member_node}" ctr -n k8s.io images tag docker.io/library/kube-ovn-central:e2e "${member_image}"
# This test user creates its own new directory. No permissions on existing
# storage, accounts or credentials are changed by the provisioner or this test.
docker exec --user 65532:65532 "${member_node}" mkdir -m 0700 /tmp/operator-member-volume
MEMBER_TEST_IMAGE="${member_image}" MEMBER_TEST_NODE="${member_node}" go run ./test/e2e/member-smoke
