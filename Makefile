IMG ?= ghcr.io/kubeovn/kube-ovn-operator:dev

.PHONY: all build test vet lint manifests chart-lint docker-build docker-push e2e

all: test build

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

manifests:
	@cmp config/crd/bases/operator.kubeovn.io_kubeovnmemberremovalapprovals.yaml charts/kube-ovn-operator/crds/operator.kubeovn.io_kubeovnmemberremovalapprovals.yaml
	@test -s config/crd/bases/operator.kubeovn.io_kubeovnmemberremovalapprovals.yaml
	@cmp config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml charts/kube-ovn-operator/crds/operator.kubeovn.io_kubeovninstallations.yaml
	@cmp config/crd/bases/operator.kubeovn.io_kubeovnrecoveryrequests.yaml charts/kube-ovn-operator/crds/operator.kubeovn.io_kubeovnrecoveryrequests.yaml
	@test -s config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml
	@test -s config/crd/bases/operator.kubeovn.io_kubeovnrecoveryrequests.yaml
	@test -s config/manager/manager.yaml
	@test -s config/rbac/role.yaml
	@test -s config/rbac/service_account.yaml
	@test -s config/samples/operator_v1alpha1_kubeovninstallation.yaml
	@grep -q '^kind: CustomResourceDefinition$$' config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml
	@grep -q '^kind: CustomResourceDefinition$$' config/crd/bases/operator.kubeovn.io_kubeovnrecoveryrequests.yaml
	@grep -q 'name: kubeovninstallations.operator.kubeovn.io' config/crd/bases/operator.kubeovn.io_kubeovninstallations.yaml
	@grep -q 'name: kubeovnrecoveryrequests.operator.kubeovn.io' config/crd/bases/operator.kubeovn.io_kubeovnrecoveryrequests.yaml

chart-lint:
	helm lint charts/kube-ovn-operator

docker-build:
	docker build -t $(IMG) .

docker-push:
	docker push $(IMG)

e2e:
	bash test/e2e/smoke.sh
