package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

// 多来源:CART 作 priority-1 优先(带 maxConcurrency)+ 后端桶 priority-0 兜底,
// 顺序正确、位置形式正确(priority-0 不输出多余字段)。
func TestResolveSourcesAndRenderRoute(t *testing.T) {
	peersByTarget := map[string][]config.Peer{
		"cart-glm":     {{IP: "10.0.0.1", Port: 8071}},
		"glm-backends": {{IP: "10.1.0.1", Port: 8050}, {IP: "10.1.0.2", Port: 8050}},
	}
	peers := ResolveSources(peersByTarget, []config.RouteSource{
		{Target: "cart-glm", Priority: 1, MaxConcurrency: 180}, // CART 优先
		{Target: "glm-backends", Priority: 0},                  // 后端兜底
	}, "")

	conf, err := RenderRoute(RouteData{Route: "glm", Listen: 18083, Peers: peers,
		Extra: map[string]string{"ttft_limit_ms": "60000", "zz_new_tunable": "9"}}) // 任意 key 原样渲染
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	wantCart := `{ "10.0.0.1", 8071, "cart-glm-0", 1, 180 },`
	wantBe1 := `{ "10.1.0.1", 8050, "glm-backends-0" },`
	wantBe2 := `{ "10.1.0.2", 8050, "glm-backends-1" },`
	// Extra 任意 key(含 openresty 以后新加的)都渲染,无需改代码
	for _, w := range []string{wantCart, wantBe1, wantBe2, "listen 18083", "ttft_limit_ms = 60000,", "zz_new_tunable = 9,"} {
		if !strings.Contains(conf, w) {
			t.Errorf("rendered conf missing %q\n---\n%s", w, conf)
		}
	}
	if strings.Index(conf, wantCart) > strings.Index(conf, wantBe1) {
		t.Errorf("CART peer should precede backend peers\n%s", conf)
	}
}

// 单 target(sources 为空)回退。
func TestResolveSourcesSingleTarget(t *testing.T) {
	peers := ResolveSources(map[string][]config.Peer{
		"glm-backends": {{IP: "10.1.0.1", Port: 8050}},
	}, nil, "glm-backends")
	conf, err := RenderRoute(RouteData{Route: "glm", Listen: 18083, Peers: peers})
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	if !strings.Contains(conf, `{ "10.1.0.1", 8050, "glm-backends-0" },`) {
		t.Errorf("single-target render wrong:\n%s", conf)
	}
}
