package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

func TestRenderMonitor(t *testing.T) {
	backends := []config.Peer{{IP: "10.1.0.1", Port: 8050}, {IP: "10.1.0.2", Port: 8050}}
	nginx := []config.Peer{{IP: "10.2.0.1", Port: 18080}}
	router := []config.Peer{{IP: "10.3.0.1", Port: 8071}}
	out := RenderMonitor("kimi-k2.6", "kimi-k2.6", "H100", "openresty", backends, nginx, router)
	for _, w := range []string{
		"service: kimi-k2.6-0 | http://10.1.0.1:8050 | kimi-k2.6 | H100",
		"service: kimi-k2.6-1 | http://10.1.0.2:8050 | kimi-k2.6 | H100",
		"nginx: openresty-0 | http://10.2.0.1:18080",
		"router: kimi-k2.6-router-0 | http://10.3.0.1:8071/workers",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q\n%s", w, out)
		}
	}
	// model 省略时用 mrName;nginx/router 为空则不出对应行
	only := RenderMonitor("m1", "", "", "", []config.Peer{{IP: "1.2.3.4", Port: 80}}, nil, nil)
	if !strings.Contains(only, "service: m1-0 | http://1.2.3.4:80 | m1 | ") {
		t.Error("empty model should fall back to mrName")
	}
	if strings.Contains(only, "nginx:") || strings.Contains(only, "router:") {
		t.Errorf("empty nginx/router should emit no lines\n%s", only)
	}
}
