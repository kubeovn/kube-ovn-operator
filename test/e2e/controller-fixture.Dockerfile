FROM kube-ovn-central:e2e
USER 0
COPY test/e2e/controller-fixture/fixture.sh /fixture/fixture.sh
RUN chmod 0755 /fixture/fixture.sh \
    && for name in ovs-appctl ovsdb-client ovn-appctl; do ln -s /fixture/fixture.sh "/fixture/${name}"; done
ENV PATH="/fixture:/opt/ovn-native:${PATH}"
USER 65532:65532
ENTRYPOINT ["/bin/sh", "-ec"]
CMD ["exec sleep 3600"]
