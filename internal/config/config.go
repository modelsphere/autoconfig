// Package config parses the autoconfig agent config (targets + sinks).
package config

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Peer is one backend endpoint. Positional openresty form: {ip, port, name[, priority[, maxConcurrency]]}.
type Peer struct {
	IP             string `json:"ip"`
	Port           int    `json:"port"`
	Name           string `json:"name"`
	Priority       int    `json:"priority"`       // openresty only; 0 = default
	MaxConcurrency int    `json:"maxConcurrency"` // openresty only; 0 = use route default
	MaxLoad        int    `json:"maxLoad"`        // cart only; 0 = sink default
}

// LuaTuple renders the positional lua peer form: "ip", port, "name"[, priority[, maxConcurrency]].
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
	return s
}

// Target = one bucket of backends discovered by a label selector. Not LWS-specific.
type Target struct {
	Name        string `json:"name"`      // logical name; referenced by sinks
	Namespace       string `json:"namespace"`
	Selector        string `json:"selector"` // k8s label selector, e.g. "app=kimi,role=leader"
	Port            int    `json:"port"`
	IncludeNotReady bool   `json:"includeNotReady"` // default false = only Ready pods
	StaticPeers     []Peer `json:"staticPeers"`
}

// Sink = one consumer (a CART instance, or an openresty with N routes). Explicit target binding.
type Sink struct {
	Kind            string `json:"kind"`            // "cart" | "openresty"
	OutputConfigMap string `json:"outputConfigMap"` // "namespace/name" the agent writes/updates

	// cart:
	Target     string `json:"target"`     // which target's backends this CART serves
	BaseConfig string `json:"baseConfig"` // path to base config.yaml WITHOUT a workers: block
	MaxLoad    int    `json:"maxLoad"`    // per-worker max_load (default 20)

	// openresty —— 两种模式(可并用):
	// (A) rewrite:读 baseConfigDir 的现成 conf,按 routeByTarget 改写其 peers 块(其余原样透传)。
	BaseConfigDir string            `json:"baseConfigDir"`
	RouteByTarget map[string]string `json:"routeByTarget"`
	// (B) template:agent 用 Go 模板给每条 route 生成【整个 conf】(dicts+server+register_route+peers)。
	//     加路由 = 加一个 routes 条目 + 一个 target,不用手写 conf。
	Template string            `json:"template"` // 模板文件路径(挂进 agent)
	Routes   []OpenrestyRoute  `json:"routes"`
}

// OpenrestyRoute = template 模式下一条路由:输出文件名 + 传给模板的 values + peer 来源。
// peer 来源二选一:
//   - target:单桶后端(简写,向后兼容)。
//   - sources:多来源(有序,带 priority)。典型 = CART 作 priority-1 优先 + 后端桶作 priority-0 兜底。
type OpenrestyRoute struct {
	Target  string                 `json:"target"`  // 单 target 简写(sources 为空时用)
	Sources []RouteSource          `json:"sources"` // 多来源(有序);非空时优先于 target
	File    string                 `json:"file"`    // 输出 ConfigMap key,如 session_route_glm.conf
	Values  map[string]interface{} `json:"values"`  // 模板里用 {{.Values.route}} / {{.Values.listen}} / 覆盖项
}

// RouteSource = 一条 route 的一个 peer 来源:某 target 的发现结果 + priority/maxConcurrency 覆盖。
// 用于「CART 优先 + 后端兜底」:sources: [{target: cart-glm, priority: 1}, {target: glm-backends, priority: 0}]。
type RouteSource struct {
	Target         string `json:"target"`
	Priority       int    `json:"priority"`       // 覆盖该来源所有 peer 的 priority(0 = 不覆盖/默认)
	MaxConcurrency int    `json:"maxConcurrency"` // 覆盖该来源所有 peer 的 maxConcurrency(0 = 不覆盖)
}

type Config struct {
	IntervalSeconds int      `json:"intervalSeconds"`
	Targets         []Target `json:"targets"`
	Sinks           []Sink   `json:"sinks"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.IntervalSeconds == 0 {
		c.IntervalSeconds = 5
	}
	for i := range c.Targets {
		// default readyOnly=true unless explicitly set false — but zero-value bool can't distinguish.
		// Convention: readyOnly defaults true; set explicitly false to include NotReady pods.
		if c.Targets[i].Port == 0 {
			return nil, fmt.Errorf("target %q: port required", c.Targets[i].Name)
		}
	}
	return &c, nil
}

// String returns a deterministic serialization (for hot-reload change detection).
func (c *Config) String() string {
	b, _ := yaml.Marshal(c)
	return string(b)
}

// TargetByName returns the target with the given name.
func (c *Config) TargetByName(name string) *Target {
	for i := range c.Targets {
		if c.Targets[i].Name == name {
			return &c.Targets[i]
		}
	}
	return nil
}
