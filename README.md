# autoconfig

从 k8s 按 **Service 自动发现后端端点**（watch 其 EndpointSlice；一个 Service = 一个「模型」桶），渲染并实时更新
**openresty**、**cache-aware-router（CART）**、可选 **monitor** 的 peer/worker/监控配置 —— 免去手工维护路由器的后端列表。
上 k8s 后 Service 背后的端点（pod IP）随扩缩容/重启变化，autoconfig 让这几处配置跟着自动收敛。

输入是 **`ModelRoute` CRD**（一个模型一条），`kubectl apply` 当场校验、`kubectl get mr` 直接看发现了几个后端。
（早期 ConfigMap 驱动的「agent 模式」已移除。）

## 工作原理

![autoconfig 架构：controller 按 Service 发现后端端点 → 写 openresty / CART / monitor 三个 ConfigMap；openresty/CART 里 reload sidecar 收 SIGHUP 热重载，monitor 自身每 60s 热加载](docs/architecture.png)

三个独立二进制 / 镜像，各司其职：

| 组件 | 镜像 | 角色 |
|---|---|---|
| **controller** | `autoconfig:0.3.30`（`cmd/`） | 唯一发现逻辑 + RBAC 一处；watch ModelRoute + EndpointSlice + Pod → 发现 → 渲染 → 写 ConfigMap + status。controller 自身 `replicas>1` 时靠 manager 的 leader 选举保证只有一个在干活。 |
| **reload sidecar** | `autoconfig-reload:0.3.30`（`cmd/reload`） | 跑在消费方 pod 里，watch 挂载的 ConfigMap 文件，变化就 `kill -HUP` 主进程（靠 `shareProcessNamespace`）。CART / openresty 收 SIGHUP 优雅重载。 |
| **hagate sidecar** | `autoconfig-hagate:0.3.30`（`cmd/hagate`） | 消费方 **master-standby**：2 副本都保持 Ready，但只有持 Lease 的 leader 给自己 pod 打 `<name>-active=true` 标签；Service selector 带这个标签 → **只有 leader 进 endpoints**。用标签而非 readiness 门控，standby 不会永久 NotReady 卡住滚动。 |

（controller 自身、hagate master-standby、reload 热重载三种机制详见下节。）

## 两个 sidecar 的实现原理

消费方 pod（openresty / CART）里除主容器外各挂两个 autoconfig sidecar：**hagate**（主备门控）+ **reload**（配置热重载）。
两者都靠 `shareProcessNamespace: true` 与主容器同 pod 协作。

### hagate —— master-standby 单活门控

**为什么要单活**：openresty / CART 是**有状态**路由器（openresty 有 session 亲和 + `active_conns` 并发计数，
CART 有 prefix-cache radix tree）。多副本同时进 Service endpoints = 缓存被打散、并发计数分裂，路由质量下降。
所以要 **2 副本主备（master-standby）**：都保持运行，但同一时刻只有一个对外收流量。

**为什么不用 readinessProbe 门控**：若让 standby 的 readiness 恒 NotReady 来挡流量，Deployment 滚动时
`maxUnavailable`/`minReady` 会把「永久 NotReady 的 standby」当成不可用 → 滚动卡死。故改用**标签门控**而非 readiness。

**机制**：每 pod 一个 hagate sidecar 参与 Lease `<name>-ha` 的 leader 选举。
- 只有持 Lease 的 leader 给**自己 pod** 打 `<name>-active=true` 标签；
- Service 的 selector 带这个标签 → **只有 leader 的 pod 进 endpoints**，standby 在池外待命；
- 消费方上游（如 openresty 的 cart 层）走 **Service ClusterIP VIP**，VIP 恒指 active leader → failover/rollout 对上游透明。

**level-triggered 自愈**：每 2s 读 pod 实际标签，与「**该不该 active**（= 是 leader **且**本地 app 端口可连）」比对，
不符就纠正 —— 标签被外部误删也能自愈；本地 app 端口连不上时即便是 leader 也主动摘标签（避免把流量导向坏 pod）。

**failover**：计划内下线（SIGTERM=删 pod/滚动/驱逐）release Lease → standby ~1-2s 接管**新**流量；本 pod **保留** active 标签作 **terminating endpoint**（deletionTimestamp），靠 Cilium graceful-terminating 把新连接导向 standby、老在途连接留在本 pod 排空（配合主容器 SIGQUIT 优雅停 + grace），**不硬摘标签 → 不 reset 在途连接**，pod 退出即自动出 endpoints。存活丢主（Lease 续约失败但 pod 没死）才摘标签离开 Service（避免 2-active）。硬崩则等 Lease TTL 过期后接管。

**monitor 不做 HA**：采集/告警是**自主轮询循环**（不接收外部流量），readiness 门控挡不住重复采集，单活无意义 →
monitor 单例（`replicas: 1` + `Recreate`），chart 不带 hagate。openresty/cart 默认开（`replicas: 2` + `ha.enabled: true`）。

### reload —— 配置热重载

**问题**：autoconfig 改写了 ConfigMap，主进程（nginx / CART）要重读配置才生效，但不能重启（会断在途长流式连接）。

**机制**：每消费方 pod 一个 reload sidecar：
- 把输出 ConfigMap **整卷挂**（非 subPath —— subPath 不随 ConfigMap 更新同步）到 `--watch` 目录，用 fsnotify 监听；
- 文件变 → 找主进程 pid（读 `/proc/*/cmdline` 匹配 `nginx: master` / `cache-aware-router`；**用 cmdline 不用 `comm`**——comm 截断 15 字符、且要避开 nginx worker）→ `kill -HUP`；
- nginx / CART 收 **SIGHUP 都是优雅重载**：坏配置只 log warning + 保留旧配置，绝不中断在途请求。

**传播**：kubelet 同步挂载的 ConfigMap 有 ~1min 延迟（AtomicWriter `..data` 原子软链切换 → reload 看到的永远是完整文件，不会读到半写）。

**openresty 侧 `--sock-dir`**：per-model server 监听 unix socket，模型删除后 nginx 不会自动 unlink 残留 `.sock`；reload 前先删掉「已无 conf 引用」的孤儿 socket。

## ModelRoute CRD

`ModelRoute`（`routing.gpucluster.io/v1alpha1`），一个模型一个对象，`kubectl apply` 当场 CEL 校验、`kubectl get mr` 看发现结果。
完整样例见 [`config/samples/modelroute-glm.yaml`](config/samples/modelroute-glm.yaml)。下表逐字段说明（✅=必填）。

**`spec` 顶层**

| 字段 | 必填 | 含义 |
|---|---|---|
| `discovery` | ✅ | 本模型的后端桶发现方式（喂 CART workers / nginx backend / monitor services） |
| `cart` | 可选 | 配了 = autoconfig 管这个 CART；省略 = nginx 直连后端（无 CART 层） |
| `nginx` | ✅ | openresty 路由：渲染 peers → `session_route_<route>.conf` |
| `monitor` | 可选 | 把发现的后端/入口/CART 也写进共享 `monitor.conf` |

**`spec.discovery`** —— 一桶后端怎么发现

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `service` | string | 与 `selector` **二选一** | EndpointSlice 发现（推荐）；支持 `ns/name` 跨 ns（裸名默认同 ModelRoute 的 ns）→ ModelRoute 可放中心 ns |
| `selector` | string | 与 `service` **二选一** | pod label 发现（没建 Service 的单机/单卡兜底） |
| `port` | int | 可选，省略自动推导 | 后端端口；service 路径从 EndpointSlice 取、selector 路径从 containerPort 取（仅单端口可推） |
| `includeNotReady` | bool | `false` | 默认只取 Ready 端点（排空中端点自动排除）；`true` = 含未 Ready |

**`spec.cart`** —— 省略整段 = 无 CART

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `service` / `selector` | string | **二选一** | CART pod 发现（供 openresty 的 cart source）；`service` 支持 `ns/name` |
| `port` | int | 省略推导 | CART 端口（EndpointSlice/containerPort 单端口自动推） |
| `outputConfigMap` | string | ✅ | 写 CART `config.yaml` 的目标 `ns/name`；底稿（server/cache/health）由 chart 的 `values.baseConfig` 建在此 CM，autoconfig 只重填 `workers` 段 |
| `maxLoad` | int | `20` | 每 worker 的 `max_load` |

**`spec.nginx`** —— openresty 路由

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `route` | string | 省略 = `metadata.name` | 路由短名 = conf 文件名 + openresty dict 名 + `<route>.sock` + 外部路径 key `/<route>/`。**字符集必须 ⊆ `[a-z0-9._-]`**（做 dispatch 路径捕获正则；含大写会派生不到 socket → 8080 打不通） |
| `peers` | list | ✅（≥1） | 有序 peer 组（见 `peers[]` 表） |
| `outputConfigMap` | string | ✅ | 输出 ConfigMap `ns/name`（多路由共享，每路由一个 key，finalizer 摘各自 key） |
| `values` | map | 可选 | 任意调优项原样渲染进 lua `register_route` 返回表（key=value）→ 加新调优项无需改代码。常用 `ttft_limit_ms` / `tps_limit_tps` / `adaptive_cc_min` / `default_max`（数字不加引号） |
| `service` | string | 可选 | nginx 入口自身的 Service `ns/name` → 供 monitor 的 `nginx:` 行 + 入口 pod 扩缩事件驱动；端口取 Service 的 dispatch 命名端口 8080 |
| `selector` | string | 可选 | nginx 入口 pod label 发现（没建 Service 兜底；与 `service` 二选一，都配则 `service` 优先） |

**`spec.nginx.values` 示例 —— 开启动态限流(自适应并发 AIMD)**

配了 `tps_limit_tps` 即对本路由 opt-in;未显式 `adaptive_cc: "false"` 时,`adaptive_cc` 按全局默认(`ADAPTIVE_CC_DEFAULT=true`)自动开。删掉 `values` 即退回不限流。

```yaml
spec:
  nginx:
    route: qwen
    service: llm-route/openresty
    outputConfigMap: llm-route/openresty-conf
    values:                       # 任意 key 原样渲染进 lua register_route opts(值必须字符串)
      tps_limit_tps: "30"         # 解码速率下限(tok/s):EWMA 低于它→AIMD 缩并发,高于它→涨(= opt-in 闸门)
      # adaptive_cc_min: "10"     # 可选:并发下限(不配 = 静态 max × 全局 min_frac 派生)
      # ttft_limit_ms: "60000"    # 可选:TTFT 软控阈值
      # adaptive_cc: "false"      # 可选:显式关自适应,走静态硬熔断
    peers:
    - { use: cart, priority: 3, maxConcurrencyFromBackend: true }
    - { use: backend, priority: 2, maxConcurrency: 100 }
    - { use: backend-svc, priority: 1 }
```

生效后 openresty 侧 `GET /<route>/_tps_status` 应见 `opt_in=true, adaptive_cc_on=true`;bodylog-exporter 的 `openresty_adaptive_cc{route}` / `openresty_tps_ewma{route}` 随之进 Prometheus。

**`spec.nginx.peers[]`** —— 有序分层（数字大=优先，高优层全 banned 才级联到低层）

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `use` | enum | ✅ `cart`\|`backend`\|`backend-svc` | 见下「三档 `use`」 |
| `priority` | int | — | openresty peer 优先级（建议 cart=3、backend=2、backend-svc=1） |
| `maxConcurrency` | int | 省略用 `values.default_max` | 该组所有 peer 的并发上限 |
| `maxConcurrencyFromBackend` | bool | 仅 `use:cart` 有意义 | `true` = cart 并发上限动态 = 后端单实例并发 × 后端数（CART 扇出到 N 后端，容量随扩缩自动变）；设了则忽略静态 `maxConcurrency`，且**要求 `backend` 组 `maxConcurrency>0`** 作乘数 |
| `probePath` | string | 省略见右 | openresty 健康探测路径覆盖（GET，状态行含 200=健康否则 ban）。`use:cart` 默认 `/health`（CART 的 `/v1/models` 是缓存端点、worker 全挂也返 200，不能当信号）；其余层默认空 = 用 route 的 `health_probe_path`（`/v1/models`）。显式设值（含给 cart 设 `/v1/models`）覆盖默认 |

三档 `use`：
- **`cart`**（priority 3）—— CART 上游，走 **CART Service 的 ClusterIP（VIP，非 pod IP）**：CART 是 master-standby，VIP 恒指 active leader → CART failover/rollout 对 openresty 透明，autoconfig 无需重写。
- **`backend`**（priority 2）—— 后端 **pod IP**（session 亲和 / least_conn / per-peer 健康的主力层）。
- **`backend-svc`**（priority 1，可选兜底）—— 后端 **Service 的 ClusterIP（VIP）静态兜底**：**autoconfig 本身宕 + 后端 rollout** 时 pod-IP 层是死 IP 又没人重写 → 若无兜底会全断；VIP 由 kube-proxy 维护、不依赖 autoconfig 存活，pod-IP 层全 banned 后级联到它 → **降级（走 kube-proxy、无亲和）但不全断**。需 `discovery.service`（selector 模式无 VIP，自动跳过）。

**`spec.monitor`** —— 可选；monitor 自身每 60s 热加载，**无 reload sidecar**（区别于 nginx/CART）。每模型一个 key，三类行：

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `outputConfigMap` | string | ✅ | 写 monitor 配置的 ConfigMap `ns/name`（多模型共享，每模型一个 key） |
| `model` | string | 省略 = `metadata.name` | `service:` 行的 model 字段（served-model-name） |
| `gpuType` | string | 省略自动推导 | `service:` 行的 gpu_type；省略 = 从后端节点 GPU label `nvidia.com/gpu.product`（GFD）推短名，推不出留空 |
| `nginx` | bool | 默认 `true`（当 `spec.nginx` 配了 service/selector） | 复用 nginx 入口发现写 monitor 的 `nginx:` 行；`false` 关 |
| `router` | bool | 默认 `true`（当配了 `spec.cart`） | 复用 `spec.cart` 发现的 CART pod 写 monitor 的 `router:` 表（`.../workers`）；`false` 关 |

三类输出行格式：`service: <name> \| <url> \| <model> \| <gpu_type>`（每后端实例一行）、`nginx: <svc>-<i> \| http://ip:8080/<route>`、`router: <name>-router-<i> \| http://ip:port/workers`。

**CEL 校验（apply 时即报错）**：① `discovery`/`cart` 的 `service` 与 `selector` 必须**恰好一个**；② `nginx.peers` 用了 `cart` 必须配 `spec.cart`；③ `cart` 用 `maxConcurrencyFromBackend` 必须给 `backend` 组配 `maxConcurrency>0`。

## 消费方接入

autoconfig 只负责把配置**写进已有的 ConfigMap**（不创建 chart、不创建 ConfigMap）；消费方各自 chart 建初始
ConfigMap + reload/hagate sidecar + Service 门控，并把对应 ConfigMap 挂进自己的 pod。引用的 `autoconfig-reload` /
`autoconfig-hagate` sidecar 镜像由本仓构建（harbor 跨仓引用）。三个消费方一览：

| 消费方 | chart 位置 | autoconfig 写的 ConfigMap → 挂载文件 | reload sidecar |
|---|---|---|---|
| **openresty** | `llm-openresty` 仓 `k8s/helm/openresty/` | `openresty-conf` → `conf.d/routes/session_route_<route>.conf` | ✅ `--process "nginx: master"` + `--sock-dir` |
| **cart**（cache_aware_router） | `cache_aware_router` 仓 `k8s/helm/` | `cart-config` → `configs/config.yaml`（只重填 `workers` 段） | ✅ `--process cache-aware-router` |
| **monitor** | `llm-monitor` 仓 `k8s/helm/monitor/` | `monitor-conf` → `conf.d/<model>.monitor.conf` | ❌ 自身每 60s 热加载 |

### openresty

- **ConfigMap 交付（为什么挂子目录）**：ConfigMap 整卷挂会覆盖整个目录，而 `.conf` 和 `lua/` 同在 `conf.d/`。
  所以把 `session_route*.conf` 移到子目录 **`conf.d/routes/`**，ConfigMap（`openresty-conf`）只挂到那里；
  `lua/` + `router_locations.inc` + `nginx.conf` + **8080 dispatch（`session_base.conf`）** 仍烤镜像。
  `nginx.conf` 的 include 从 `conf.d/*.conf` 改成 `conf.d/routes/*.conf`（`lua_package_path` 不变）。
- **路径路由（单一对外端口）**：
  - 镜像 baked 一个 `listen 8080` 的 dispatch server，按请求路径首段 `/<route>/`
  运行时派生到 per-model server 的 unix socket（`<prefix>/sock/<route>.sock`）——单一对外端口、零映射表。
  - per-model server 只 `listen unix:.../<route>.sock`（不占 TCP 端口），故 `spec.nginx.route` = 外部路径 key
  = socket 名；autoconfig 生成的 `session_route_<route>.conf` 里就是这个 socket listen。
  - dispatch 把打 8080 的
  真实客户端 IP 经 `X-Real-IP` 透传，per-model `set_real_ip_from unix:` 还原 `$remote_addr` →
  `allow 127.0.0.1` 的调参端点仍只对 in-pod 本地开放。
- **reload sidecar**：`--process "nginx: master"` + **`--sock-dir`**（reload 前删掉「无 conf 引用」的孤儿 `.sock`——
  模型删除后 nginx 不会自动 unlink 残留 socket）。

### cart

- **ConfigMap 交付**：整卷挂 `cart-config` → cart 启动 `-c configs/config.yaml`。底稿（`server`/`cache`/`health`）
  由 chart 的 `values.baseConfig` 建在此 ConfigMap，autoconfig 只重填 `workers` 段。
- **reload sidecar**：`--process cache-aware-router`。

### monitor

- **ConfigMap 交付**：整卷挂 `monitor-conf` → `conf.d/<model>.monitor.conf`（多模型共享，每模型一个 key）。
- **无 reload sidecar**：monitor 自身每 60s 热加载 monitor.conf，不需要 SIGHUP（区别于 openresty/cart）。

### reload sidecar 接入（openresty / cart 通用）

机制见上「reload —— 配置热重载」。接入 = 消费方 pod 加一个 `autoconfig-reload` 容器
（`shareProcessNamespace: true` 才能发 SIGHUP + 输出 ConfigMap **整卷挂**到 `--watch`），args：
```yaml
# openresty
args: ["--watch","/watch","--process","nginx: master","--sock-dir","/usr/local/openresty/nginx/sock"]
# cart
args: ["--watch","/watch","--process","cache-aware-router"]
```

## 用法

**Helm 部署 controller（推荐）** —— chart 在 `deploy/helm/autoconfig/`（含 CRD + SA/RBAC + Deployment）：
```bash
helm upgrade --install autoconfig deploy/helm/autoconfig -n llm-route --create-namespace
kubectl apply -f config/samples/modelroute-glm.yaml
kubectl get mr -A                                   # NAME/BACKENDS/CART/READY/AGE
```
常用 values：`image.tag`、`replicas`（>1 leader 选举 HA）、`leaderElection`、`modelRoutes`（直接在 chart 里声明路由）。
CRD 放在 chart 的 `crds/`（Helm install-once、`helm uninstall` **不删**，保护已有 ModelRoute；升级 CRD schema 用
`make install` 或 `kubectl apply -f config/crd/bases/...`）。

不用 helm 时，用 kustomize（同源生成物，见下方「开发布局」）：
```bash
make install    # 装 CRD（config/crd/bases）
make deploy     # 起 controller + RBAC（config/default）
```

**⚠️ 卸载顺序：先删 ModelRoute，再 `helm uninstall`。** ModelRoute 带 finalizer（`routing.gpucluster.io/cleanup`），
要 controller 在跑才能摘。若先 uninstall（删了 controller）再删 ModelRoute / namespace，ModelRoute 会卡住、拖住
namespace/CRD 删除。正确：`kubectl delete mr --all -A` → `helm uninstall`。（chart 里用 `modelRoutes` 声明的 ModelRoute
由 helm 托管，uninstall 前会随 release 删除、controller 还在 → 自动摘 finalizer，无此问题。）已卡住的补救：
`kubectl patch mr <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'`。

## 构建（三个镜像）

多阶段 build（照 `llm-openresty/Dockerfile.bodylog`）：golang builder 从**国内 goproxy**（默认 `mirrors.tencent.com/go`，
`GOPROXY` 是 `ARG` 可 `--build-arg` 换 aliyun 等）拉依赖，**不再 vendor**；`go.sum` 入库 + `GOSUMDB=off` 保证可复现，`GOTOOLCHAIN=local` 防联网拉工具链：
```bash
docker build              -t harbor.4pd.io/hardcore-tech/autoconfig:<tag>        .   # controller(cmd/)
docker build -f Dockerfile.reload -t harbor.4pd.io/hardcore-tech/autoconfig-reload:<tag> .   # reload sidecar
docker build -f Dockerfile.hagate -t harbor.4pd.io/hardcore-tech/autoconfig-hagate:<tag> .   # hagate sidecar
```

**CI（`.gitlab-ci.yml`）：打 git tag 自动 build+push 三个镜像**（`public-buildx` runner，docker 已 login harbor；
该 runner 到国内 goproxy `mirrors.tencent.com/go` 可达——同 runner 上 `llm-openresty` CI #416384 build:bodylog 实测通过）。

本地快速验证：`GOPROXY=https://mirrors.tencent.com/go/,direct GOSUMDB=off go build ./cmd/...`。

## 验证状态（别混淆两层）

- **CRD controller**（现版）：k8s-cpu-20 + **mock 后端**已验——发现分桶、**写出的 cart-config/openresty-conf/monitor-conf
  内容正确**、status、scale 跟随、fail-safe、删除清理（finalizer/ownerRef）、`make install`/`make deploy`。即验到「autoconfig
  写对 ConfigMap」为止。
- **真 openresty + 真 CART + 真 monitor 端到端接入**（经 helm chart，真 reload/serve、master-standby failover）：
  `test/e2e/real_helm_e2e.sh` **已跑通 ALL PASS**（k8s-cpu-20：真 reload 热更、路径路由 unix socket、`openresty -t`、scale 跟随、monitor service/nginx/router 行；live pod 验 openresty `terminationGracePeriodSeconds`/`preStop`）；另见 `ha_failover_check.sh`，真后端（kimi LWS / vllm）见 `real_kimi_e2e.sh` / `verify_v12_vllm.sh`。

## 开发布局（kubebuilder / operator-sdk v4）

标准 operator 布局：`PROJECT` + `Makefile` + `api/v1alpha1`（带 kubebuilder marker 的类型）+ `internal/controller`（reconciler）
+ `internal/{discovery,sink,hagate,reload}` + `config/`（kustomize：crd/rbac/manager/default/samples）。

- **改了 `api/` 类型或 `+kubebuilder:` marker 后**，跑生成、提交生成物（CI 不跑生成，只编译）：
  ```bash
  make generate manifests    # controller-gen 生成 deepcopy + config/crd/bases + config/rbac/role.yaml,并同步 CRD 到 helm/crds
  make test                  # 生成 + fmt + vet + go test
  ```
  工具用 `go run ...@version`（见 Makefile），不装二进制、不入库。
- **部署两条路都可**：  
  - 生产用 **Helm**（`deploy/helm/autoconfig`）；
  - kustomize 用 `make deploy`（`config/default`）。
  - 两者的 CRD/RBAC 同源（都来自 `config/` 的生成物）。

## 注意（踩坑）

- **ConfigMap 必须整卷挂**（非 subPath）才会随更新自动同步；kubelet 同步有 **~1min 延迟**——对 peer 更新可接受
  （health-timer + proxy_next_upstream 兜过渡），对 CART 反而是天然去抖（reload 会重建 radix tree）。
- **fail-safe**：发现结果为空绝不写空（CART 拒绝空 workers；openresty 会丢全部流量）。
- **reload 找 pid 只比 argv[0]**（不是整条 cmdline，也不用 comm——comm 截断 15 字符）：否则 sidecar 自己的
  `--process nginx: master` 参数会自匹配。规则：argv[0] 相等 / basename 相等 / 以 match 开头（nginx master 的
  argv[0] = `nginx: master process ...`）。
- **env 名别撞 k8s Service 注入**：若有名为 `cart` 的 Service，k8s 会注入 `CART_PORT=tcp://...`；autoconfig 的 env 前缀统一 `PS_`。
