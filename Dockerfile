# autoconfig 镜像 —— 多阶段 build:golang builder(harbor)离线编译 → 打进 python:3.12-alpine。
#
# 为什么多阶段 + vendor:CI 的 public-buildx runner 只有 go1.17.6(太老)且够不到外网。
# harbor 有 library/golang:1.23.3-alpine3.20(满足 go.mod go1.23);依赖已 vendor(vendor/,入库),
# `go build -mod=vendor` 全程不联网 → runner 离线也能编。本机手工 build 也走同一条 `docker build .`。
#
#   docker build -t harbor.4pd.io/hardcore-tech/autoconfig:<tag> .
#   docker push harbor.4pd.io/hardcore-tech/autoconfig:<tag>
# 打 git tag 自动 build+push,见 .gitlab-ci.yml。
FROM harbor.4pd.io/library/golang:1.23.3-alpine3.20 AS build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor -ldflags="-s -w" -o /out/autoconfig ./cmd/autoconfig

FROM harbor.4pd.io/hardcore-tech/python:3.12-alpine
COPY --from=build /out/autoconfig /usr/local/bin/autoconfig
ENTRYPOINT ["/usr/local/bin/autoconfig"]
