package sink

import (
	"fmt"
	"os"
	"strings"

	"autoconfig/internal/config"
)

// CartSink renders one CART's config.yaml (workers block) for a single target.
type CartSink struct{ s config.Sink }

func (c *CartSink) Kind() string          { return "cart" }
func (c *CartSink) Name() string          { return "cart:" + c.s.Target }
func (c *CartSink) ConfigMapNS() string   { ns, _ := splitNSName(c.s.OutputConfigMap); return ns }
func (c *CartSink) ConfigMapName() string { _, n := splitNSName(c.s.OutputConfigMap); return n }

const cartDefaultBase = "server:\n  host: \"0.0.0.0\"\n  port: 6700\n"

func (c *CartSink) Render(peersByTarget map[string][]config.Peer) (Rendered, error) {
	peers := nameUnnamed(peersByTarget[c.s.Target], c.s.Target)
	base := cartDefaultBase
	if c.s.BaseConfig != "" {
		b, err := os.ReadFile(c.s.BaseConfig)
		if err != nil {
			return nil, fmt.Errorf("read baseConfig %s: %w", c.s.BaseConfig, err)
		}
		base = string(b)
	}
	return Rendered{"config.yaml": RenderCart(base, peers, c.s.MaxLoad)}, nil
}

// RenderCart 把 base config.yaml(不含 workers)+ 发现的后端渲染成完整 config.yaml。
// ConfigMap-agent(CartSink)与 CRD controller 共用这一个纯函数。maxLoad<=0 用默认 20。
func RenderCart(base string, peers []config.Peer, maxLoad int) string {
	if maxLoad == 0 {
		maxLoad = 20
	}
	var b strings.Builder
	b.WriteString(strings.TrimRight(base, "\n"))
	b.WriteString("\n\nworkers:\n")
	for _, p := range peers {
		ml := p.MaxLoad
		if ml == 0 {
			ml = maxLoad
		}
		fmt.Fprintf(&b, "  - url: \"http://%s:%d\"\n    max_load: %d\n", p.IP, p.Port, ml)
	}
	return b.String()
}
