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

// RouteSource:openresty 一条 route 的一个 peer 来源。
type RouteSource struct {
	// Use:"cart"(引用本模型的 CART pod)或 "backend"(引用 discovery 的后端桶)。
	// +kubebuilder:validation:Enum=cart;backend
	Use string `json:"use"`
	// Priority:openresty peer 优先级(CART 优先=1,后端兜底=0)。
	Priority int `json:"priority,omitempty"`
	// MaxConcurrency:该来源 peer 的并发上限覆盖。
	MaxConcurrency int `json:"maxConcurrency,omitempty"`
}

// OpenrestySpec:本模型的 openresty 路由。
type OpenrestySpec struct {
	// Route:路由短名(dict/register 用),如 "glm"。
	Route string `json:"route"`
	// Listen:server 监听端口。
	Listen int `json:"listen"`
	// Sources:有序 peer 来源(如 cart 优先 + backend 兜底)。
	// +kubebuilder:validation:MinItems=1
	Sources []RouteSource `json:"sources"`
	// OutputConfigMap:openresty 输出 ConfigMap("ns/name",多路由共享,每路由一个 key)。
	OutputConfigMap string `json:"outputConfigMap"`
	// Values:传给模板的额外值(如 ttft_limit_ms、tps_limit_tps、default_max)。
	Values map[string]string `json:"values,omitempty"`
}

// MonitorSpec:可选;有 = autoconfig 把发现的后端写进 monitor 的 services 列表。
// monitor 自身每 60s 热加载 monitor.conf,无需 reload sidecar(不同于 openresty/CART)。
type MonitorSpec struct {
	// OutputConfigMap:autoconfig 写 monitor 配置的 ConfigMap("ns/name",多模型共享,每模型一个 key)。
	OutputConfigMap string `json:"outputConfigMap"`
	// Model:monitor service 行的 model 字段(served-model-name);省略用 metadata.name。
	Model string `json:"model,omitempty"`
	// GPUType:monitor service 行的 gpu_type 字段(如 H100 / B300 / H200)。
	GPUType string `json:"gpuType,omitempty"`
	// Nginx:可选;openresty 入口发现(service/selector 二选一 + port)。autoconfig 探测其 pod,
	// 生成 monitor 的 nginx: 行(name 用 Service,跨模型同 Service 自动 dedup)。
	Nginx *Discovery `json:"nginx,omitempty"`
	// Router:可选,默认 true(当配了 spec.cart);把探测到的 CART pod 写进 monitor 的 router: 表(.../workers)。设 false 关闭。
	Router *bool `json:"router,omitempty"`
}

// ModelRouteSpec 是一个模型的完整路由绑定。
// +kubebuilder:validation:XValidation:rule="!self.openresty.sources.exists(s, s.use == 'cart') || has(self.cart)",message="openresty.sources 用了 cart,但没配 spec.cart"
type ModelRouteSpec struct {
	// Discovery:本模型的后端桶(喂 CART 的 workers、openresty 的 backend 来源、monitor 的 services)。
	Discovery Discovery `json:"discovery"`
	// Cart:可选;有 = autoconfig 管这个 CART,无 = openresty 直连后端。
	Cart *CartSpec `json:"cart,omitempty"`
	// Openresty:openresty 路由。
	Openresty OpenrestySpec `json:"openresty"`
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
