FROM gcr.io/distroless/static:nonroot
COPY network-probe /network-probe
USER 65532:65532
ENTRYPOINT ["/network-probe"]
