package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Discovery 描述「一桶后端」怎么发现:service(EndpointSlice)与 selector(pod label)恰好其一。
type Discovery struct {
	// Service:走 EndpointSlice 发现(推荐);该 Service 需只选服务端点(LWS Service 配成只选 leader)。
	Service string `json:"service,omitempty"`
	// Selector:走 pod label 发现(兜底:没建 Service 的单机/单卡)。
	Selector string `json:"selector,omitempty"`
	// Port:服务端口(配 pod IP)。
	Port int `json:"port"`
	// IncludeNotReady:默认 false 只取 Ready 端点(排空中端点自动排除)。
	IncludeNotReady bool `json:"includeNotReady,omitempty"`
}

// ConfigMapKeyRef 引用一个 ConfigMap 的某个 key(如 CART 的 base config.yaml)。
type ConfigMapKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// CartSpec:本模型的 CART 实例。autoconfig 发现后端 → 写 CART 的 workers;并发现 CART pod 供 openresty 引用。
// 省略整个 cart 段 = openresty 直连后端(无 CART)。
type CartSpec struct {
	// CART pod 的发现(供 openresty 的 cart source);service/selector 二选一。
	Service  string `json:"service,omitempty"`
	Selector string `json:"selector,omitempty"`
	Port     int    `json:"port"`
	// OutputConfigMap:autoconfig 写 CART 的 config.yaml 到这("ns/name")。
	OutputConfigMap string `json:"outputConfigMap"`
	// BaseConfigRef:CART config.yaml 的静态底稿(不含 workers);省略用内置默认。
	BaseConfigRef *ConfigMapKeyRef `json:"baseConfigRef,omitempty"`
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
	Sources []RouteSource `json:"sources"`
	// OutputConfigMap:openresty 输出 ConfigMap("ns/name",多路由共享,每路由一个 key)。
	OutputConfigMap string `json:"outputConfigMap"`
	// Values:传给模板的额外值(如 ttft_limit_ms、tps_limit_tps、default_max)。
	Values map[string]string `json:"values,omitempty"`
}

// ModelRouteSpec 是一个模型的完整路由绑定。
type ModelRouteSpec struct {
	// Discovery:本模型的后端桶(喂 CART 的 workers 和 openresty 的 backend 来源)。
	Discovery Discovery `json:"discovery"`
	// Cart:可选;有 = autoconfig 管这个 CART,无 = openresty 直连后端。
	Cart *CartSpec `json:"cart,omitempty"`
	// Openresty:openresty 路由。
	Openresty OpenrestySpec `json:"openresty"`
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
