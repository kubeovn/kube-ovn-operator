FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/operator ./cmd/operator
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/network-probe ./cmd/network-probe
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/member-prepare ./cmd/member-prepare
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/member-run ./cmd/member-run

FROM build AS central-test-build
RUN CGO_ENABLED=0 go test -c -o /out/central.test ./internal/central

FROM docker.io/kubeovn/kube-ovn:v1.16.13@sha256:03ca6c879a95a73faa12d9531a8fbfc7b2f5f038c939847fa19a5dda2e425e2b AS central-runtime
USER 0
RUN set -eu; mkdir /opt/ovn-native; \
    for native_tool in ovsdb-tool ovsdb-client ovsdb-server ovs-appctl ovn-northd ovn-appctl; do \
      install -m 0755 "$(command -v "$native_tool")" "/opt/ovn-native/$native_tool"; \
    done
ENV PATH="/opt/ovn-native:${PATH}"
COPY --from=build /out/member-prepare /member-prepare
COPY --from=build /out/member-run /member-run
USER 65532:65532
ENTRYPOINT ["/member-run"]
CMD []

FROM central-runtime AS central-test
USER 0
RUN set -eu; mkdir /opt/ovn-test-bin; \
    for native_tool in ovsdb-tool ovsdb-client ovsdb-server ovs-appctl ovn-northd ovn-appctl; do \
      ln -s "$(command -v "$native_tool")" "/opt/ovn-test-bin/$native_tool"; \
    done
COPY --from=central-test-build /out/central.test /central.test
ENV OVSDB_TEST_BIN=/opt/ovn-test-bin \
    OVN_TEST_BIN=/opt/ovn-test-bin \
    OVN_TEST_SCHEMA_DIR=/usr/share/ovn \
    CENTRAL_TEST_PRIVILEGED_SERVER=/usr/sbin/ovsdb-server \
    TMPDIR=/tmp
USER 65532:65532
ENTRYPOINT ["/central.test"]
CMD ["-test.run=TestLiveOVN", "-test.v", "-test.timeout=2m"]

FROM gcr.io/distroless/static:nonroot AS operator
COPY --from=build /out/operator /operator
COPY --from=build /out/network-probe /network-probe
COPY --from=build /out/member-prepare /member-prepare
COPY --from=build /out/member-run /member-run
USER 65532:65532
ENTRYPOINT ["/operator"]
