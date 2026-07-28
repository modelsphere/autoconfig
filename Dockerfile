# autoconfig 镜像 —— 二进制在本机交叉编译好(GOOS=linux GOARCH=amd64 CGO_ENABLED=0),这里只打包。
# 为什么不在镜像里 go build:内网 CI runner / chat 拉不到 golang 基础镜像;本机 go 交叉编译成静态二进制最省事。
#
# 构建(在 scripts/k8s-llm/autoconfig/ 下,binary 已 build 到 ./autoconfig):
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o autoconfig ./cmd/autoconfig
#   docker build -t harbor.4pd.io/hardcore-tech/autoconfig:0.1.0 .
#   docker push harbor.4pd.io/hardcore-tech/autoconfig:0.1.0
FROM harbor.4pd.io/hardcore-tech/python:3.12-alpine
COPY autoconfig /usr/local/bin/autoconfig
ENTRYPOINT ["/usr/local/bin/autoconfig"]
