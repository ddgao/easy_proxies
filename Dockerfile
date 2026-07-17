FROM --platform=$BUILDPLATFORM golang:1.24 AS builder
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
ARG GOPROXY=https://proxy.golang.org,direct
RUN go env -w GOPROXY=${GOPROXY} && go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -tags "with_utls with_quic with_grpc with_wireguard with_gvisor with_clash_api" -o easy_proxies ./cmd/easy_proxies

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/easy_proxies /app
WORKDIR /app
COPY --from=builder /src/easy_proxies /usr/local/bin/easy_proxies
COPY config.example.yaml /app/config.example.yaml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
# GeoIP: 1221, Pool/Hybrid: 2323, Lease Gateway: 2330, Management: 9091, Multi-port/Hybrid: 24000-24200
EXPOSE 1221 2323 2330 9091 24000-24200
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--config", "/etc/easy_proxies/config.yaml"]
