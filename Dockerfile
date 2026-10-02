# autoconfig: the ModelRoute controller (cmd/). Multi-stage: a Go builder
# compiles the binary, which is copied into a small runtime image.
#
#   docker build -t autoconfig:dev .
#
# Dependencies are not vendored: go.sum is committed, so `go mod download` is
# reproducible. GOTOOLCHAIN=local keeps go from fetching a newer toolchain
# because of the version in go.mod.
#
# The base images and the Go module proxy are build args. Their defaults are
# public (Docker Hub, the upstream Go proxy), so a fresh clone builds as is.
# Behind a firewall, point them at a mirror:
#   --build-arg GO_BASE=<registry>/library/golang:1.23.3-alpine3.20
#   --build-arg RUNTIME_BASE=<registry>/library/python:3.12-alpine
#   --build-arg GOPROXY=<your-goproxy>,direct
#
# A version tag builds and publishes the image: .github/workflows/release.yml.
ARG GO_BASE=golang:1.23.3-alpine3.20
ARG RUNTIME_BASE=python:3.12-alpine
ARG GOPROXY=https://proxy.golang.org,direct

FROM ${GO_BASE} AS build
ARG GOPROXY
# Set by BuildKit for each platform being built; the defaults apply to the legacy builder.
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV GOPROXY=${GOPROXY} GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/autoconfig ./cmd

FROM ${RUNTIME_BASE}
COPY --from=build /out/autoconfig /usr/local/bin/autoconfig
ENTRYPOINT ["/usr/local/bin/autoconfig"]
