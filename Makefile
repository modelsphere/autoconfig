# autoconfig —— kubebuilder/operator-sdk 风格 Makefile。
# 工具用 `go run ...@version`(无需装二进制;仅开发时联网拉,不进 CI/镜像)。生成物提交进 repo,CI 只编译。

IMG        ?= harbor.4pd.io/hardcore-tech/autoconfig:latest
RELOAD_IMG ?= harbor.4pd.io/hardcore-tech/autoconfig-reload:latest

CONTROLLER_GEN_VERSION ?= v0.16.4
KUSTOMIZE_VERSION      ?= v5.4.3
CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
KUSTOMIZE      = go run sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

HELM_CRD = deploy/helm/autoconfig/crds/routing.gpucluster.io_modelroutes.yaml

.PHONY: all
all: build

##@ 代码生成(改了 api/ 类型或 controller 的 +kubebuilder marker 后跑,再提交生成物)

.PHONY: manifests
manifests: ## 生成 CRD + RBAC(从 marker),并同步 CRD 到 Helm chart。
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths=./... output:rbac:artifacts:config=config/rbac
	cp config/crd/bases/routing.gpucluster.io_modelroutes.yaml $(HELM_CRD)

.PHONY: generate
generate: ## 生成 deepcopy(zz_generated.deepcopy.go)。
	$(CONTROLLER_GEN) object paths=./api/...

##@ 开发

.PHONY: fmt
fmt: ; go fmt ./...
.PHONY: vet
vet: ; go vet -mod=vendor ./...
.PHONY: test
test: manifests generate fmt vet ## 生成 + 静态检查 + 单测。
	go test -mod=vendor ./...

##@ 构建(离线 vendored;与 CI 的 docker build 同源)

.PHONY: build
build: ## 交叉编译两个二进制到 bin/。
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor -ldflags="-s -w" -o bin/autoconfig ./cmd
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor -ldflags="-s -w" -o bin/reload ./cmd/reload

.PHONY: docker-build
docker-build: ## 构建 controller + reload 两个镜像。
	docker build -t $(IMG) .
	docker build -f Dockerfile.reload -t $(RELOAD_IMG) .

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
