FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/operator ./cmd/operator
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/network-probe ./cmd/network-probe
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/member-prepare ./cmd/member-prepare

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/operator /operator
COPY --from=build /out/network-probe /network-probe
COPY --from=build /out/member-prepare /member-prepare
USER 65532:65532
ENTRYPOINT ["/operator"]
