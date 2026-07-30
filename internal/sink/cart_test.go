package sink

import (
	"strings"
	"testing"
)

// CartBase:从现有 config.yaml 剥掉 workers: 段,保留底稿(server/cache/health)。
func TestCartBase(t *testing.T) {
	cfg := "server:\n  host: \"0.0.0.0\"\n  port: 8071\ncache: { threshold: 0.3 }\n\nworkers:\n  - url: \"http://1.2.3.4:8050\"\n    max_load: 20\n"
	base := CartBase(cfg)
	if strings.Contains(base, "workers") || strings.Contains(base, "1.2.3.4") {
		t.Errorf("base 应不含 workers 段:\n%s", base)
	}
	if !strings.Contains(base, "port: 8071") || !strings.Contains(base, "threshold: 0.3") {
		t.Errorf("base 应保留 server/cache:\n%s", base)
	}
	// 无 workers 段则原样返回
	if CartBase("server: {}\n") != "server: {}\n" {
		t.Error("无 workers 段应原样返回")
	}
}
