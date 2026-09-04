# syntax=docker/dockerfile:1
ARG REGISTRY=docker.io
FROM ${REGISTRY}/library/golang:1.26-alpine AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc \

    GOPROXY=http://jj-lab.temp.svc.cluster.local/pkgs/go \
    GOSUMDB=off \
    GONOSUMDB=abep.dev/sdk,abep.dev/sdk/nats,abep.dev/sdk/ws \
    GOFLAGS=-mod=mod
RUN sed -i 's|dl-cdn.alpinelinux.org|mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
# manifest.yaml is embedded into the binary via go:embed — no sidecar needed.
COPY manifest.yaml ./
RUN CGO_ENABLED=0 go build -o /out/memory-extension .

FROM ${REGISTRY}/library/alpine:3.24
RUN sed -i 's|dl-cdn.alpinelinux.org|mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk add --no-cache ca-certificates
COPY --from=build /out/memory-extension /usr/local/bin/memory-extension
EXPOSE 8080
ENTRYPOINT ["memory-extension"]
