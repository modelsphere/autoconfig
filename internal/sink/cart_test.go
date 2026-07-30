package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

// RenderCart:YAML 解析底稿 → 覆盖 workers 键(不受「workers 放最后」约束,底稿其余键原样保留)。
func TestRenderCart(t *testing.T) {
	// 底稿:workers 在中间、后面还有 cache —— 字符串截断法会丢 cache,YAML 法不会
	base := "server:\n  port: 8071\nworkers: []\ncache:\n  threshold: 0.3\n"
	out, err := RenderCart(base, []config.Peer{{IP: "10.1.0.1", Port: 8050}}, 20)
	if err != nil {
		t.Fatalf("RenderCart: %v", err)
	}
	for _, w := range []string{"port: 8071", "threshold: 0.3", "workers:", "http://10.1.0.1:8050", "max_load: 20"} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q\n%s", w, out)
		}
	}
	// base 为空用内置默认
	if o, _ := RenderCart("", nil, 0); !strings.Contains(o, "port: 6700") {
		t.Error("空 base 应用内置默认 6700")
	}
	// per-worker max_load 覆盖
	o, _ := RenderCart("server: {}", []config.Peer{{IP: "1.2.3.4", Port: 80, MaxLoad: 5}}, 20)
	if !strings.Contains(o, "max_load: 5") {
		t.Errorf("per-worker max_load 应生效\n%s", o)
	}
}
