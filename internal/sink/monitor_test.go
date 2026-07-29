package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

func TestRenderMonitor(t *testing.T) {
	out := RenderMonitor("kimi-k2.6", "kimi-k2.6", "H100", []config.Peer{
		{IP: "10.1.0.1", Port: 8050}, {IP: "10.1.0.2", Port: 8050},
	})
	for _, w := range []string{
		"service: kimi-k2.6-0 | http://10.1.0.1:8050 | kimi-k2.6 | H100",
		"service: kimi-k2.6-1 | http://10.1.0.2:8050 | kimi-k2.6 | H100",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q\n%s", w, out)
		}
	}
	// model 省略时用 mrName
	if !strings.Contains(RenderMonitor("m1", "", "", []config.Peer{{IP: "1.2.3.4", Port: 80}}),
		"service: m1-0 | http://1.2.3.4:80 | m1 | ") {
		t.Error("empty model should fall back to mrName")
	}
}
