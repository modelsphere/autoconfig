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

// OpenrestyRoute = template 模式下一条路由:target(哪桶后端)+ 输出文件名 + 传给模板的 values。
type OpenrestyRoute struct {
	Target string                 `json:"target"`
	File   string                 `json:"file"`   // 输出 ConfigMap key,如 session_route_glm.conf
	Values map[string]interface{} `json:"values"` // 模板里用 {{.Values.route}} / {{.Values.listen}} / 覆盖项
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
