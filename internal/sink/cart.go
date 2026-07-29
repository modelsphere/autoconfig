package sink

import (
	"fmt"
	"strings"

	"autoconfig/internal/config"
)

const cartDefaultBase = "server:\n  host: \"0.0.0.0\"\n  port: 6700\n"

// RenderCart 把 base config.yaml(不含 workers)+ 发现的后端渲染成完整 config.yaml。
// base 为空用内置默认;maxLoad<=0 用默认 20。
func RenderCart(base string, peers []config.Peer, maxLoad int) string {
	if base == "" {
		base = cartDefaultBase
	}
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
