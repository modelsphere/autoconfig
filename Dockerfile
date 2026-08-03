# autoconfig 镜像 —— 多阶段:golang builder 从国内 goproxy 拉依赖编译 → 打进 python:3.12-alpine。
#
# 照 llm-openresty/Dockerfile.bodylog:依赖走【国内 goproxy 镜像】(mirrors.tencent.com/go —— public-buildx
# runner 实测可达,见 llm-openresty CI #416384),不再 vendor。go.sum 入库 + GOSUMDB=off → 仍可复现;
# GOTOOLCHAIN=local 防 go 因 go.mod 版本联网拉新工具链。GOPROXY 是 ARG,可 --build-arg 换 aliyun 等。
#
#   docker build -t harbor.4pd.io/hardcore-tech/autoconfig:<tag> .
#   docker push harbor.4pd.io/hardcore-tech/autoconfig:<tag>
# 打 git tag 自动 build+push,见 .gitlab-ci.yml。
ARG GO_BASE=harbor.4pd.io/library/golang:1.23.3-alpine3.20
ARG GOPROXY=https://mirrors.tencent.com/go/,direct

FROM ${GO_BASE} AS build
ARG GOPROXY
ENV GOPROXY=${GOPROXY} GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux GOARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/autoconfig ./cmd

FROM harbor.4pd.io/hardcore-tech/python:3.12-alpine
COPY --from=build /out/autoconfig /usr/local/bin/autoconfig
ENTRYPOINT ["/usr/local/bin/autoconfig"]
