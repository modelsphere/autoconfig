# autoconfig CRD 化改造方案（讨论稿）

> 把 autoconfig 从「一个大 ConfigMap 配置 + 轮询 agent」升级成 **CRD + controller-runtime**。
> **加法式、低风险**:发现 / 渲染 / reload 全部复用现有代码,只把「输入」从 ConfigMap 换成 CR,并多写一段 `status`。

## 1. 为什么

| 现状（ConfigMap 驱动） | CRD 化后 |
|---|---|
| 配置写错**运行时才报**,还可能被 fail-safe 静默吞掉 | **apply 当场校验**(OpenAPI + CEL),错误提交时暴露 |
| 发现了几个后端 / 上次 reload —— **翻 agent 日志** | **status 子资源** `kubectl get/describe` 直接查 |
| 一个大 ConfigMap 一个团队维护 | **每模型一个 CR**:GitOps、per-ns RBAC 分权 |
| 删一条路由,残留输出 key 手动收 | **ownerReferences 级联 GC**,删 CR 自动清 |
| 单副本轮询 | controller-runtime 白拿 **informer / leader 选举(HA) / metrics** |
| ConfigMap YAML 无版本纪律 | CRD **版本 + conversion**,schema 安全演进 |
| 与 `LLMScaler`(CRD)风格不一 | 整套 operator 风格统一,CR 成为集成点 |

## 2. CRD 设计

`routing.4pd.io/v1alpha1` · `ModelRoute`(namespaced,一个模型/一条路由一个)。

```yaml
apiVersion: routing.4pd.io/v1alpha1
kind: ModelRoute
metadata: { name: glm-5.1-fp8, namespace: glm }
spec:
  discovery:                       # service 与 selector 恰好其一
    service: glm-leader            # → EndpointSlice(推荐)
    # selector: "app=glm,role=leader"   # → pod label(兜底)
    port: 8050
    includeNotReady: false
  openresty:
    route: glm
    listen: 18083
    sources:                       # 有序:CART 优先 + 后端兜底
      - { kind: cart,    service: cart-glm, port: 8071, priority: 1, maxConcurrency: 180 }
      - { kind: backend, priority: 0 }
    outputConfigMap: openresty/openresty-conf
  cart:
    baseConfigRef: { name: base-cart, key: config.base.yaml }
    outputConfigMap: glm/cart-config
    maxLoad: 20
status:                            # controller 回写(只读)
  backends: 6
  cartPeers: 1
  ready: true
  observedGeneration: 4
  lastReloadTime: "2026-07-29T01:50:00Z"
  conditions:
    - { type: Ready, status: "True", reason: Synced }
```

**校验(apply 时挡掉)** —— CRD `x-kubernetes-validations`(CEL):
```
- rule: "has(self.discovery.service) != has(self.discovery.selector)"
  message: "discovery: service 和 selector 必须恰好给一个"
- rule: "self.openresty.sources.size() > 0"
  message: "openresty.sources 不能为空"
```
**printer columns** → `kubectl get modelroute`:
```
NAME          TARGET       BACKENDS  CART  READY  LASTSYNC
glm-5.1-fp8   glm-leader   6         1     True   12s
kimi-k2.6     (label)      4         0     True   12s
```

## 3. Controller 架构(controller-runtime)

```
Manager(leader 选举)
└── ModelRoute Reconciler
     Watches: ModelRoute(主)  +  Owns: 输出 ConfigMap  +  Watches: EndpointSlice/Pod →(映射回相关 ModelRoute 入队)
     Reconcile(rb):
        1) discover        —— 复用 internal/discovery(EndpointSlice / label)
        2) render          —— 复用 internal/sink(cart / openresty,多来源 priority)
        3) write ConfigMap —— 带 ownerReference(级联 GC)+ diff/fail-safe(照旧)
        4) update status   —— backends/cartPeers/conditions/lastReloadTime(新增)
```
**reload 仍是 sidecar**(独立的 reload 小程序/镜像(cmd/reload),inotify→SIGHUP)——**完全不变**。也可选让 controller 经 pods/exec 触发,但 sidecar 更解耦,建议保留。

**关键**:`internal/discovery` 和 `internal/sink` 两个包**原样复用**,只是调用方从「轮询 agent」换成「reconcile 循环」。新增的只有:CRD 类型、reconciler 壳、status 回写。

## 4. 迁移路径(加法、可灰度、可回退)

1. **装 CRD + controller**,与现 agent **并存**(先不接管)。
2. 现 ConfigMap 配置 → 每模型一个 `ModelRoute`(写个一次性转换脚本)。
3. controller **先只读**:渲染出结果和现 agent 的输出**做 diff 比对**,确认一致。
4. **切写**:controller 接管输出 ConfigMap 的写入;下线老 agent。
5. **消费方零改**:输出 ConfigMap 格式不变 → openresty / CART pod + reload sidecar 一个字不用动。

## 5. 保持不变(复用面)

- 输出 ConfigMap 的**格式**(cart 的 `config.yaml` + workers、openresty 的 `session_route_*.conf`)。
- 消费方 pod、**reload sidecar**、`shareProcessNamespace`。
- **多来源 CART 优先 + 后端兜底**、**EndpointSlice/label 双发现**、diff、fail-safe。

## 6. 取舍

- **成本**:CRD 安装(cluster-scoped)、controller-runtime 依赖、镜像变大、若加 admission webhook 需证书(helm-selfsigned 我们已趟过 rdma-injector)。
- **何时不值**:模型很少、配置很稳 —— ConfigMap 已够,CRD 是过度设计。
- **最值钱的两条是 ① apply 校验 + ② status**;若只想先要这俩,可先做,不必一步到位上全套 operator 生态。

## 7. 已定的决策

1. **CART 发现归 autoconfig**(不改 CART):autoconfig 外部发现后端 → 写 CART 的 `config.yaml`(workers)+ SIGHUP。因此 `ModelRoute` 有完整的 `cart` 段。
2. **单个 CR**(`ModelRoute`,一个模型一个),`cart` 段**可选**——省略 = openresty 直连后端。理由:CART 归 autoconfig 后,「模型 X 的后端桶」这**一个 `discovery` 同时喂 CART 的 workers 和 openresty 的兜底 source**,单 CR = 单一真相;现状 1 模型=1 CART=1 route。将来若出现「一个 CART 被多条路由共享 / 一条路由 fan 多个 CART / 团队分权」再拆(加法式:`sources` 支持 `cartRef` 引用独立 CartBinding)。
3. **reload 仍走 sidecar**(独立的 reload 小程序/镜像(cmd/reload)),controller 不碰 reload。

## 8. 输出 ConfigMap 的归属与清理

- **cart 的 outputConfigMap**:每模型专属 → 设 **ownerReference**,删 `ModelRoute` 时 k8s 级联 GC。
- **openresty 的 outputConfigMap**:**多条路由共享一个**(每路由一个 key)→ **不设 ownerRef**(否则删一条会连累整个)。改用 **finalizer**:删 `ModelRoute` 时只移除它那一个 key。
