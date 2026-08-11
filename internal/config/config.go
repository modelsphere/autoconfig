// Package config holds the shared value types used by discovery and the render helpers.
package config

import "fmt"

// Peer is one backend endpoint. Positional openresty form: {ip, port, name[, priority[, maxConcurrency]]},
// 外加可选命名字段 probe = "<健康探测路径>"(见 ProbePath)。
type Peer struct {
	IP             string
	Port           int
	Name           string
	Priority       int    // openresty only; 0 = default
	MaxConcurrency int    // openresty only; 0 = use route default
	MaxLoad        int    // cart only; 0 = sink default
	ProbePath      string // openresty only; 空=用 route 默认(/v1/models)。cart peer 默认 "/health"
	Node           string // 该端点所在节点名(发现时填);仅用于推导 GPU,不渲染进配置
	GPU            string // 该端点所在节点的 GPU 型号短名(A100/H100/H200/…);推不出为空
}

// LuaTuple renders the positional lua peer form: "ip", port, "name"[, priority[, maxConcurrency]],
// 再拼可选命名字段 , probe = "<path>" / , gpu = "<型号>"(命名字段不占位置,不影响前面的位置元组)。
// priority/maxConcurrency 只在非零时才输出(与 openresty peers 的位置约定一致);
// maxConcurrency 非零必须先补 priority 占位(第 4 位),否则位置错位。
func (p Peer) LuaTuple() string {
	s := fmt.Sprintf("%q, %d, %q", p.IP, p.Port, p.Name)
	if p.Priority != 0 || p.MaxConcurrency != 0 {
		s += fmt.Sprintf(", %d", p.Priority)
		if p.MaxConcurrency != 0 {
			s += fmt.Sprintf(", %d", p.MaxConcurrency)
		}
	}
	if p.ProbePath != "" {
		s += fmt.Sprintf(", probe = %q", p.ProbePath)
	}
	if p.GPU != "" {
		s += fmt.Sprintf(", gpu = %q", p.GPU)
	}
	return s
}

// Target = one bucket of backends. Discovered via EITHER an EndpointSlice-backed Service (Service)
// OR a pod label selector (Selector) — exactly one.
type Target struct {
	Namespace       string
	Service         string // Service 名 → 走 EndpointSlice 发现(原生就绪/终止语义)
	Selector        string // pod label selector → 走 pod 发现(兜底:没建 Service 的单机/单卡)
	Port            int    // 显式端口;0 = 自动(PortName 非空按名取,否则唯一端口推导)
	PortName        string // 可选:按 Service 端口名取端口(多端口 Service 消歧,如 openresty 的 "dispatch");Port>0 时忽略
	IncludeNotReady bool   // default false = only Ready endpoints
}

// RouteSource = 一条 openresty route 的一个 peer 来源:某 target 的发现结果 + priority/maxConcurrency 覆盖。
// 典型「CART 优先 + 后端兜底」:sources: [{Target: cart, Priority: 1}, {Target: backend, Priority: 0}]。
type RouteSource struct {
	Target         string
	Priority       int
	MaxConcurrency int
	ProbePath      string // 该层健康探测路径覆盖;空=不覆盖(用 route 默认 /v1/models)。cart 层默认 "/health"
}
