# syntax=docker/dockerfile:1
ARG REGISTRY=rucoder-artifact.temp.10.199.64.20.nip.io
FROM ${REGISTRY}/golang:1.26-alpine AS build
ARG HTTP_PROXY=http://mihomo.develop.svc.cluster.local:7890
ARG HTTPS_PROXY=http://mihomo.develop.svc.cluster.local:7890
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io \
    GOINSECURE=forgejo.develop.10.199.64.20.nip.io \
    GOPRIVATE=forgejo.develop.10.199.64.20.nip.io \
    GOPROXY=http://rucoder-artifact.temp.svc.cluster.local/pkgs/go \
    GOSUMDB=off \
    GONOSUMDB=abep.dev/sdk,abep.dev/sdk/nats,abep.dev/sdk/ws \
    GOFLAGS=-mod=mod
RUN apk add --no-cache git \
    && git config --global http.sslVerify false \
    && git config --global url."https://root:devpassword@forgejo.develop.10.199.64.20.nip.io/".insteadOf "https://forgejo.develop.10.199.64.20.nip.io/"
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY manifest.yaml ./
RUN CGO_ENABLED=0 go build -o /out/memory-extension .

FROM ${REGISTRY}/alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /out/memory-extension /usr/local/bin/memory-extension
COPY --from=build /build/manifest.yaml /manifest.yaml
EXPOSE 8080
ENTRYPOINT ["memory-extension"]
