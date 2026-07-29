# autoconfig

从 k8s 自动发现后端 pod(按 label 分桶成「模型」),渲染并更新 **openresty** 和 **cache-aware-router(CART)**
的 peer/worker 配置 —— 免去手工维护路由器的后端列表。上 k8s 后后端 pod IP 随扩缩容/重启变化,
autoconfig 让路由器配置跟着实时收敛。

## 架构:CRD controller + 独立 reload sidecar

```
┌──── autoconfig controller(Deployment,watch ModelRoute + 发现后端,RBAC 一处,leader 选举 HA)────┐
│ 每个 ModelRoute(一个模型一条):按 discovery 发现后端(EndpointSlice/label,只取 Ready)          │
│ 渲染:cart → config.yaml 的 workers;openresty → peers(CART 优先+后端兜底);monitor → services  │
│ diff(变了才写)+ fail-safe(发现为空→保留上次)→ 写进各输出 ConfigMap + 回写 status              │
└──────────┬──────────────────────────┬──────────────────────────┬──────────────────────────────────┘
   ConfigMap│(整卷挂,kubelet ~1min)ConfigMap│                ConfigMap│
   ┌────────▼──── CART pod ────┐ ┌──────▼── openresty pod ──┐ ┌────▼── monitor pod ──┐
   │ [cart] 读 config.yaml     │ │ [openresty] conf.d/routes │ │ [monitor] 60s 自动    │
   │ [reload sidecar] SIGHUP   │ │ [reload sidecar] SIGHUP    │ │   热加载,无需 sidecar  │
   │   shareProcessNamespace   │ │   shareProcessNamespace    │ │                        │
   └───────────────────────────┘ └────────────────────────────┘ └────────────────────────┘
```

- **controller**(`autoconfig`,镜像 `autoconfig`):唯一发现逻辑 + RBAC 一处;watch ModelRoute → 发现 → 渲染 → 写 ConfigMap + status。
- **reload sidecar**(**独立小程序** `cmd/reload`,镜像 `autoconfig-reload`):watch 挂载的 ConfigMap 文件,
  变化就 `kill -HUP` 主进程(靠 `shareProcessNamespace`)。CART/openresty 收 SIGHUP 优雅重载;monitor 不需要
  (自身 60s 热加载)。**controller 与 reload 是两个独立二进制/镜像**,不再是同一二进制的模式。

已在真集群 + 真 `cache_aware_router` + mock 后端端到端验证:多模型分桶、scale 精准跟随、
不变不 reload、空→保留上次(fail-safe)、删除清理(finalizer/ownerRef)。

> 配置一律用 **`ModelRoute` CRD**(一个模型一条),详见下方「CRD 模式」+ [`docs/crd-design.md`](docs/crd-design.md)。
> (早期的 ConfigMap 驱动「agent 模式」已移除。)

### openresty 侧:只把 `.conf` 放 ConfigMap + 一行 include 改动
ConfigMap 整卷挂会覆盖整个目录,而 `.conf` 和 `lua/` 同在 `conf.d/`。所以把 **session_route*.conf 移到
子目录 `conf.d/routes/`**,ConfigMap 挂到那里;`lua/` + `router_locations.inc` + `nginx.conf` 仍烤镜像。
openresty `nginx.conf` 把 `include conf.d/*.conf;` 改成 `include conf.d/routes/*.conf;`(lua_package_path 不变)。
—— 消费方(openresty pod)把 controller 输出的 openresty ConfigMap 挂到 `conf.d/routes/` 即可,autoconfig 侧无关。

## 用法:一个模型一个 ModelRoute

autoconfig controller 的输入是 **`ModelRoute`(routing.4pd.io/v1alpha1)**,一个模型一个对象;`kubectl apply` 当场校验、
`kubectl get modelroute` 直接看发现了几个后端/CART/ready。**发现(`internal/discovery`)、渲染(`internal/sink`)、
reload sidecar 全部复用**,只是「输入」变 CR、多回写 `status`。设计见 [`docs/crd-design.md`](docs/crd-design.md)。

**Helm 部署(推荐)** —— chart 在 `deploy/helm/autoconfig/`(含 CRD + SA/RBAC + Deployment):
```bash
helm upgrade --install autoconfig deploy/helm/autoconfig -n autoconfig --create-namespace
kubectl apply -f config/samples/modelroute-glm.yaml
kubectl get mr -A                                   # NAME/BACKENDS/CART/READY/AGE
```
常用 values:`image.tag`、`replicas`(>1 leader 选举 HA)、`leaderElection`、`modelRoutes`(直接在 chart 里声明路由)。
CRD 放在 chart 的 `crds/`(Helm install-once、`helm uninstall` **不删**,保护已有 ModelRoute;升级 CRD schema 用
`make install` 或 `kubectl apply -f config/crd/bases/...`)。

**⚠️ 卸载顺序:先删 ModelRoute,再 `helm uninstall`。** ModelRoute 带 finalizer
(`routing.4pd.io/cleanup`),要 controller 在跑才能摘。若先 uninstall(删了 controller)再删 ModelRoute /
namespace,ModelRoute 会卡住、拖住 namespace/CRD 删除。正确:`kubectl delete mr --all -A` → `helm uninstall`。
(chart 里用 `modelRoutes` 声明的 ModelRoute 由 helm 托管,`helm uninstall` 前会随 release 删除,controller
还在 → 自动摘 finalizer,无此问题。)已卡住的补救:
`kubectl patch mr <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'`。

裸 manifest(不想用 helm 时):
```bash
kubectl apply -f config/crd/bases/routing.4pd.io_modelroutes.yaml     # 装 CRD
kubectl apply -f deploy/controller.yaml             # 起 controller
```
一个 `ModelRoute` 同时驱动 **CART 的 workers**(cart-config,专属 → ownerRef 级联 GC)和 **openresty 的 peers**
(openresty-conf,多路由共享 → finalizer 摘 key);`cart` 段可选(省略 = openresty 直连后端)。

## 构建(两个镜像)

**多阶段 build,依赖已 vendor(`vendor/` 入库),全程离线**——不联网、不预置二进制:

```bash
docker build -t harbor.4pd.io/hardcore-tech/autoconfig:<tag> .                       # controller(cmd/autoconfig)
docker build -f Dockerfile.reload -t harbor.4pd.io/hardcore-tech/autoconfig-reload:<tag> .  # reload sidecar(cmd/reload,独立小镜像)
```

**CI(`.gitlab-ci.yml`):打 git tag 自动 build+push 两个镜像**(`autoconfig` + `autoconfig-reload`,各 `:<tag>`+`:latest`;`public-buildx` runner,docker 已 login harbor)。该 runner 只 go1.17.6 且够不到外网,故用 vendor 离线 + golang builder 从 harbor 拉。改依赖后 `go mod vendor` 重新入库。

本地快速验证:`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor ./cmd/...`。

## 开发布局(kubebuilder / operator-sdk v4)

标准 operator 布局:`PROJECT`(项目元数据)+ `Makefile`(生成/构建/部署目标)+ `api/v1alpha1`(带 kubebuilder marker
的类型)+ `internal/controller`(reconciler)+ `config/`(kustomize:crd/rbac/manager/default/samples)。

- **改了 `api/` 类型或 `+kubebuilder:` marker 后**,跑生成、提交生成物(CI 不跑生成,只编译):
  ```bash
  make generate manifests    # controller-gen 生成 deepcopy + config/crd/bases + config/rbac/role.yaml,并同步 CRD 到 helm/crds
  make test                  # 生成 + fmt + vet + go test
  ```
  工具用 `go run ...@version`(见 Makefile),不装二进制、不进 vendor/CI。
- **部署两条路都可**:生产用 **Helm**(`deploy/helm/autoconfig`);kustomize 用 `make deploy`(`config/default`)。
  两者的 CRD/RBAC 同源(都来自 `config/` 的生成物)。

## 部署

- **controller**:`deploy/helm/autoconfig`(推荐)或 `deploy/controller.yaml` + `config/crd/bases/routing.4pd.io_modelroutes.yaml`。
- **reload sidecar**:消费方 pod(openresty / CART)里加一个容器,镜像 `autoconfig-reload`,
  `args: ["--watch","/watch","--process","nginx: master"]`(或 `cache-aware-router`)+ `shareProcessNamespace: true`
  + 把对应输出 ConfigMap **整卷挂**(非 subPath——subPath 不随 ConfigMap 更新)。monitor 不需要 sidecar。

## 注意(踩坑)

- **ConfigMap 必须整卷挂**(非 subPath)才会随更新自动同步;kubelet 同步有 **~1min 延迟**——对 peer
  更新可接受(health-timer + proxy_next_upstream 兜过渡),对 CART 反而是天然去抖(reload 会重建 radix tree)。
- **env 名别撞 k8s Service 注入**:若有名为 `cart` 的 Service,k8s 会注入 `CART_PORT=tcp://...`;
  autoconfig 的 env 前缀统一 `PS_`。
- **找 pid 用 `/proc/pid/cmdline` 不用 comm**(comm 截断 15 字符);openresty 匹配 master(`nginx: master`)。
- **fail-safe**:发现结果为空绝不写空(CART 拒绝空 workers;openresty 会丢全部流量)。
