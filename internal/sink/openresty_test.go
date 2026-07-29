package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

// 验证 template 模式下多来源:CART 作 priority-1 优先(带 maxConcurrency)+ 后端桶 priority-0 兜底,
// 顺序正确、位置形式正确(priority-0 不输出多余字段)。
func TestTemplateMultiSourceCartPreferred(t *testing.T) {
	s := config.Sink{
		Kind:            "openresty",
		OutputConfigMap: "openresty/openresty-conf",
		Template:        "../../deploy/openresty-route.tmpl",
		Routes: []config.OpenrestyRoute{{
			File:   "session_route_glm.conf",
			Values: map[string]interface{}{"route": "glm", "listen": 18083},
			Sources: []config.RouteSource{
				{Target: "cart-glm", Priority: 1, MaxConcurrency: 180}, // CART 优先
				{Target: "glm-backends", Priority: 0},                  // 后端兜底
			},
		}},
	}
	sk, err := New(s)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	peersByTarget := map[string][]config.Peer{
		"cart-glm":     {{IP: "10.0.0.1", Port: 8071}},
		"glm-backends": {{IP: "10.1.0.1", Port: 8050}, {IP: "10.1.0.2", Port: 8050}},
	}
	out, err := sk.Render(peersByTarget)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	conf := out["session_route_glm.conf"]
	if conf == "" {
		t.Fatalf("no conf rendered; got keys %v", keys(out))
	}

	// CART 优先 peer:priority 1 + maxConcurrency 180
	wantCart := `{ "10.0.0.1", 8071, "cart-glm-0", 1, 180 },`
	// 后端兜底:priority 0 → 只有 ip/port/name 三元
	wantBe1 := `{ "10.1.0.1", 8050, "glm-backends-0" },`
	wantBe2 := `{ "10.1.0.2", 8050, "glm-backends-1" },`
	for _, w := range []string{wantCart, wantBe1, wantBe2} {
		if !strings.Contains(conf, w) {
			t.Errorf("rendered conf missing %q\n---\n%s", w, conf)
		}
	}
	// 顺序:CART 必须排在后端前面(优先兜底语义)
	if strings.Index(conf, wantCart) > strings.Index(conf, wantBe1) {
		t.Errorf("CART peer should precede backend peers\n---\n%s", conf)
	}
}

// 单 target(向后兼容)仍工作,且默认 priority 0 不输出多余字段。
func TestTemplateSingleTargetBackCompat(t *testing.T) {
	s := config.Sink{
		Kind:            "openresty",
		OutputConfigMap: "openresty/openresty-conf",
		Template:        "../../deploy/openresty-route.tmpl",
		Routes: []config.OpenrestyRoute{{
			Target: "glm-backends",
			File:   "session_route_glm.conf",
			Values: map[string]interface{}{"route": "glm", "listen": 18083},
		}},
	}
	sk, _ := New(s)
	out, err := sk.Render(map[string][]config.Peer{
		"glm-backends": {{IP: "10.1.0.1", Port: 8050}},
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(out["session_route_glm.conf"], `{ "10.1.0.1", 8050, "glm-backends-0" },`) {
		t.Errorf("single-target render wrong:\n%s", out["session_route_glm.conf"])
	}
}

func keys(m Rendered) []string {
	var k []string
	for x := range m {
		k = append(k, x)
	}
	return k
}
