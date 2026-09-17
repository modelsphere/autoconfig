# autoconfig —— kubebuilder/operator-sdk 风格 Makefile。
# 工具用 `go run ...@version`(无需装二进制;仅开发时联网拉,不进 CI/镜像)。生成物提交进 repo,CI 只编译。

IMG        ?= harbor.4pd.io/hardcore-tech/autoconfig:latest
RELOAD_IMG ?= harbor.4pd.io/hardcore-tech/autoconfig-reload:latest

# Dockerfile 里 GO_BASE / RUNTIME_BASE / GOPROXY 三个 ARG 的默认值是【公网】
# (Docker Hub + proxy.golang.org),保证外部用户开箱可 build。内网构建覆盖成 harbor
# 缓存与国内 goproxy —— 下面是内网默认值,走公网时 `make docker-build BUILD_ARGS=`。
BUILD_ARGS ?= --build-arg GO_BASE=harbor.4pd.io/library/golang:1.23.3-alpine3.20 \
              --build-arg RUNTIME_BASE=harbor.4pd.io/hardcore-tech/python:3.12-alpine \
              --build-arg GOPROXY=https://mirrors.tencent.com/go/,direct

CONTROLLER_GEN_VERSION ?= v0.16.4
KUSTOMIZE_VERSION      ?= v5.4.3
CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
KUSTOMIZE      = go run sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

HELM_CRD   = deploy/helm/autoconfig/crds/routing.gpucluster.io_modelroutes.yaml
HELM_RULES = deploy/helm/autoconfig/files/controller-rules.yaml

.PHONY: all
all: build

##@ 代码生成(改了 api/ 类型或 controller 的 +kubebuilder marker 后跑,再提交生成物)

.PHONY: manifests
manifests: ## 生成 CRD + RBAC(从 marker),并同步 CRD + RBAC rules 到 Helm chart(单一来源)。
	@# crd 只扫 ./api/v1alpha1 —— 【不能用 ./api/...】:api/inference/v1alpha1 是别人的 CRD
	@# (LLMSLORequirement)的只读类型,我们不拥有它、不该发布它的 CRD;扫进来会产出一份
	@# group/kind 全空的 config/crd/bases/_.yaml 垃圾文件。
	$(CONTROLLER_GEN) crd paths=./api/v1alpha1 output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
	cp config/crd/bases/routing.gpucluster.io_modelroutes.yaml $(HELM_CRD)
	@# RBAC 单一来源:kubebuilder marker → config/rbac/role.yaml → 抽出 rules 块同步进 helm chart
	@# (helm ClusterRole 模板 .Files.Get 这个文件,名字/labels 仍由模板套)。改权限只改 marker。
	mkdir -p $(dir $(HELM_RULES))
	awk '/^rules:/{p=1;next} p' config/rbac/role.yaml > $(HELM_RULES)

.PHONY: generate
generate: ## 生成 deepcopy(zz_generated.deepcopy.go)。
	$(CONTROLLER_GEN) object paths=./api/...

##@ 开发

.PHONY: fmt
fmt: ; go fmt ./...
.PHONY: vet
vet: ; go vet ./...
.PHONY: test
test: manifests generate fmt vet ## 生成 + 静态检查 + 单测。
	go test ./...

##@ 构建(module 模式,依赖走 goproxy;与 CI 的 docker build 同源)。本机 goproxy 不通时设 GOPROXY=https://mirrors.tencent.com/go/,direct

.PHONY: build
build: ## 交叉编译两个二进制到 bin/。
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/autoconfig ./cmd
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/reload ./cmd/reload

.PHONY: docker-build
docker-build: ## 构建 controller + reload 两个镜像。
	docker build $(BUILD_ARGS) -t $(IMG) .
	docker build $(BUILD_ARGS) -f Dockerfile.reload -t $(RELOAD_IMG) .

##@ 部署(kustomize 路径;生产推荐 Helm,见 deploy/helm)

.PHONY: install
install: manifests ## 只装 CRD。
	$(KUSTOMIZE) build config/crd | kubectl apply -f -
.PHONY: uninstall
uninstall: ## 卸 CRD。
	$(KUSTOMIZE) build config/crd | kubectl delete --ignore-not-found -f -
.PHONY: deploy
deploy: manifests ## 装 CRD + RBAC + controller。
	$(KUSTOMIZE) build config/default | kubectl apply -f -
.PHONY: undeploy
undeploy: ## 卸载。
	$(KUSTOMIZE) build config/default | kubectl delete --ignore-not-found -f -
