package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Discovery 描述「一桶后端」怎么发现:service(EndpointSlice)与 selector(pod label)恰好其一。
// +kubebuilder:validation:XValidation:rule="has(self.service) != has(self.selector)",message="discovery: service 和 selector 必须恰好给一个"
type Discovery struct {
	// Service:走 EndpointSlice 发现(推荐);该 Service 需只选服务端点(LWS Service 配成只选 leader)。
	// 支持 "ns/name" 跨 ns 发现(裸名默认与 ModelRoute 同 ns),让 ModelRoute 可放中心 ns。
	Service string `json:"service,omitempty"`
	// Selector:走 pod label 发现(兜底:没建 Service 的单机/单卡)。
	Selector string `json:"selector,omitempty"`
	// Port:后端服务端口(配 pod IP)。省略则自动推导:service 路径从 EndpointSlice 取、selector 路径从 containerPort 取(仅单端口可推)。
	// +optional
	Port int `json:"port,omitempty"`
	// IncludeNotReady:默认 false 只取 Ready 端点(排空中端点自动排除)。
	IncludeNotReady bool `json:"includeNotReady,omitempty"`
}

// CartSpec:本模型的 CART 实例。autoconfig 发现后端 → 写 CART 的 workers;并发现 CART pod 供 openresty 引用。
// 省略整个 cart 段 = openresty 直连后端(无 CART)。
// +kubebuilder:validation:XValidation:rule="has(self.service) != has(self.selector)",message="cart: service 和 selector 必须恰好给一个"
type CartSpec struct {
	// CART pod 的发现(供 openresty 的 cart source);service/selector 二选一。service 支持 "ns/name" 跨 ns。
	Service  string `json:"service,omitempty"`
	Selector string `json:"selector,omitempty"`
	// Port:CART 端口。省略则从 EndpointSlice/containerPort 自动推导(单端口)。
	// +optional
	Port int `json:"port,omitempty"`
	// OutputConfigMap:autoconfig 写 CART 的 config.yaml 到这("ns/name")。
	// 底稿(server/cache/health)由 chart 的 values.baseConfig 建在此 ConfigMap 里;autoconfig 只重填 workers 段。
	OutputConfigMap string `json:"outputConfigMap"`
	// MaxLoad:每 worker 的 max_load,默认 20。
	MaxLoad int `json:"maxLoad,omitempty"`
}

// RoutePeer:openresty 一条 route 的一组 peer 来源(cart 组 / backend 组)。
type RoutePeer struct {
	// Use:"cart"(CART 上游,走 CART Service 的 ClusterIP)、"backend"(discovery 的后端桶,pod IP)、
	// 或 "backend-svc"(后端 Service 的 ClusterIP VIP,作最低优先级【静态兜底】——operator 宕+rollout 时
	// pod-IP 层全死也不全断,降级走 kube-proxy;需 discovery.service,selector 模式无 VIP 会被跳过)。
	// +kubebuilder:validation:Enum=cart;backend;backend-svc
	Use string `json:"use"`
	// Priority:openresty peer 优先级(CART 优先=1,后端兜底=0)。
	Priority int `json:"priority,omitempty"`
	// MaxConcurrency:该组所有 peer 的并发上限;省略用 values.default_max。
	MaxConcurrency int `json:"maxConcurrency,omitempty"`
	// MaxConcurrencyFromBackend:仅 use:cart 有意义。true = cart 的并发上限动态 = 后端单实例并发 × 后端数
	// (CART 扇出到 N 个后端,总容量=各后端容量之和,随后端扩缩自动变)。设了它就忽略静态 MaxConcurrency。
	// 后端单实例并发取 use:backend 组的 maxConcurrency —— CEL 校验强制此时它必须 > 0(见 ModelRouteSpec)。
	// +optional
	MaxConcurrencyFromBackend bool `json:"maxConcurrencyFromBackend,omitempty"`
	// ProbePath:该层 openresty 健康探测路径覆盖(GET <path> 状态行含 200=健康,否则 ban)。
	// 省略时:use:cart 默认 "/health"(cache_aware_router 的 /v1/models 是缓存端点、worker 全挂也返 200
	// 不能当健康信号;/health 是 worker-aware 的),其余层默认空=用 route 的 health_probe_path(/v1/models)。
	// 显式设值(含对 cart 设为 "/v1/models")覆盖默认。
	// +optional
	ProbePath string `json:"probePath,omitempty"`
}

// NginxSpec:本模型的 nginx(openresty)路由 —— 渲染 peers 到 session_route_<route>.conf。
type NginxSpec struct {
	// Route:路由短名(= conf 文件名 session_route_<route>.conf + openresty dict 名 + unix socket 名 <route>.sock
	// + 外部路径 key /<route>/)。省略 = 用 metadata.name(k8s 名恒小写,天然合规)。
	// **字符集必须 ⊆ [a-z0-9._-]**:它要做 dispatch 的路径捕获正则 `^/(?<rkey>[a-z0-9._-]+)/`,含大写/其它字符
	// → dispatch 派生不到 <route>.sock → 该模型经 8080 永远打不通。故加 Pattern 强校验。
	// +optional
	// +kubebuilder:validation:Pattern=`^[a-z0-9._-]+$`
	Route string `json:"route,omitempty"`
	// Peers:有序 peer 组(如 cart 优先 + backend 兜底)。
	// +kubebuilder:validation:MinItems=1
	Peers []RoutePeer `json:"peers"`
	// OutputConfigMap:输出 ConfigMap("ns/name",多路由共享,每路由一个 key)。
	OutputConfigMap string `json:"outputConfigMap"`
	// Values:任意调优项,原样渲染进 lua register_route 返回表(key = value)。nginx 加新调优项无需改代码。
	// 常用:ttft_limit_ms、tps_limit_tps、adaptive_cc_min、default_max。值按 lua 字面量原样写(数字不加引号)。
	Values map[string]string `json:"values,omitempty"`
	// Service:可选;nginx 入口自身的 Service("ns/name")→ 供 monitor 的 nginx: 行 + 入口 pod 扩缩事件驱动。
	// 端口取 Service 的 dispatch 命名端口(8080,路径路由统一入口)。与 selector 二选一(都配则 service 优先)。
	Service string `json:"service,omitempty"`
	// Selector:可选;nginx 入口的 pod label 发现(没建 Service 时的兜底)。
	Selector string `json:"selector,omitempty"`
}

// MonitorSpec:可选;把发现的后端/入口/CART 写进共享 monitor.conf(每模型一个 key)。
// monitor 自身每 60s 热加载 monitor.conf,无需 reload sidecar(不同于 nginx/CART)。
type MonitorSpec struct {
	// OutputConfigMap:autoconfig 写 monitor 配置的 ConfigMap("ns/name",多模型共享,每模型一个 key)。
	OutputConfigMap string `json:"outputConfigMap"`
	// Model:monitor service 行的 model 字段(served-model-name);省略 = metadata.name。
	Model string `json:"model,omitempty"`
	// GPUType:monitor service 行的 gpu_type 字段(如 H100 / A100 / H200)。省略 = 从后端所在节点的
	// GPU label(nvidia.com/gpu.product,GFD 打)自动推导成短名;推不出(无 label / 无 GFD)则留空。显式配则覆盖。
	GPUType string `json:"gpuType,omitempty"`
	// Nginx:可选,默认 true(当 spec.nginx 配了 service/selector);复用 nginx 入口发现,写 monitor 的 nginx: 行
	//(端口用 spec.nginx.listen,每端口 = 一个模型,name = 本模型)。设 false 关闭。
	Nginx *bool `json:"nginx,omitempty"`
	// Router:可选,默认 true(当配了 spec.cart);复用 spec.cart 发现的 CART pod,写 monitor 的 router: 表(.../workers)。设 false 关闭。
	Router *bool `json:"router,omitempty"`
}

// ModelRouteSpec 是一个模型的完整路由绑定。
// +kubebuilder:validation:XValidation:rule="!self.nginx.peers.exists(s, s.use == 'cart') || has(self.cart)",message="nginx.peers 用了 cart,但没配 spec.cart"
// +kubebuilder:validation:XValidation:rule="!self.nginx.peers.exists(s, s.use == 'cart' && has(s.maxConcurrencyFromBackend) && s.maxConcurrencyFromBackend) || self.nginx.peers.exists(s, s.use == 'backend' && has(s.maxConcurrency) && s.maxConcurrency > 0)",message="cart 用了 maxConcurrencyFromBackend,必须给 backend 组配 maxConcurrency(> 0)作乘数"
type ModelRouteSpec struct {
	// Discovery:本模型的后端桶(喂 CART 的 workers、nginx 的 backend 来源、monitor 的 services)。
	Discovery Discovery `json:"discovery"`
	// Cart:可选;有 = autoconfig 管这个 CART,无 = nginx 直连后端。
	Cart *CartSpec `json:"cart,omitempty"`
	// Nginx:nginx(openresty)路由。
	Nginx NginxSpec `json:"nginx"`
	// Monitor:可选;有 = 把发现的后端也写进 monitor 的 services(monitor 60s 自热加载,无 sidecar)。
	Monitor *MonitorSpec `json:"monitor,omitempty"`
}

// ModelRouteStatus 是 controller 回写的观测状态。
type ModelRouteStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Backends           int                `json:"backends"`
	CartPeers          int                `json:"cartPeers"`
	Ready              bool               `json:"ready"`
	LastSyncTime       *metav1.Time       `json:"lastSyncTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mr
// +kubebuilder:printcolumn:name="Backends",type=integer,JSONPath=`.status.backends`
// +kubebuilder:printcolumn:name="CART",type=integer,JSONPath=`.status.cartPeers`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelRoute 把「一个模型的后端」自动同步到 openresty 的 peers 与 CART 的 workers。
type ModelRoute struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelRouteSpec   `json:"spec,omitempty"`
	Status ModelRouteStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ModelRouteList is a list of ModelRoute.
type ModelRouteList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelRoute `json:"items"`
}
