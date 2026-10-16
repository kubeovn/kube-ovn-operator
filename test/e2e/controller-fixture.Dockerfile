FROM kube-ovn-central:e2e
USER 0
COPY test/e2e/controller-fixture/start.sh /fixture/start.sh
RUN chmod 0755 /fixture/start.sh
ENV PATH="/opt/ovn-native:${PATH}"
USER 65532:65532
ENTRYPOINT ["/bin/sh", "-ec"]
CMD ["exec sleep 3600"]
