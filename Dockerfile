# autoconfig controller image. Multi-stage: a Go builder, then python:3.12-alpine.
#
# Dependencies come from GOPROXY (no vendor/). go.sum is committed and GOSUMDB=off, so
# builds stay reproducible; GOTOOLCHAIN=local keeps go from fetching a newer toolchain
# because of go.mod.
#
#   docker build -t autoconfig:dev .
#
# The base images and GOPROXY are build args and default to public sources (Docker
# Hub, proxy.golang.org). Behind a registry mirror or a Go proxy, override them:
#   --build-arg GO_BASE=<mirror>/golang:1.23.3-alpine3.20
#   --build-arg RUNTIME_BASE=<mirror>/python:3.12-alpine
#   --build-arg GOPROXY=<goproxy>,direct
ARG GO_BASE=golang:1.23.3-alpine3.20
ARG RUNTIME_BASE=python:3.12-alpine
ARG GOPROXY=https://proxy.golang.org,direct

FROM ${GO_BASE} AS build
ARG GOPROXY
# TARGETARCH is set by buildx to the platform being built, so a multi-platform build
# compiles each image for its own arch. It was hardcoded to amd64, which put an amd64
# binary into the published arm64 image. A plain `docker build` leaves it empty and Go
# then builds for the host, as before.
ARG TARGETARCH
ENV GOPROXY=${GOPROXY} GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/autoconfig ./cmd

FROM ${RUNTIME_BASE}
COPY --from=build /out/autoconfig /usr/local/bin/autoconfig
ENTRYPOINT ["/usr/local/bin/autoconfig"]
