# Scratch image holding only the static release binary. Build with `task image`, which runs `task dist` first.
FROM scratch
ARG TARGETARCH
ARG VERSION=dev
LABEL org.opencontainers.image.title="butterfly-sn" \
      org.opencontainers.image.description="MQTT-SN gateway" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.source="https://github.com/nhermosilla14/butterfly-sn" \
      org.opencontainers.image.licenses="MIT"
COPY dist/linux-${TARGETARCH}/butterfly-sn /butterfly-sn
USER 65534:65534
EXPOSE 5684/udp
ENTRYPOINT ["/butterfly-sn"]
CMD ["-config", "/etc/butterfly-sn/config.yaml"]
