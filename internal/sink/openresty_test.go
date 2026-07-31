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
		{Target: "cart-glm", Priority: 1, MaxConcurrency: 180, ProbePath: "/health"}, // CART 优先,探 /health(worker-aware)
		{Target: "glm-backends", Priority: 0},                                        // 后端兜底,不带 probe → 探 /v1/models
	}, "")

	conf, err := RenderRoute(RouteData{Route: "glm", Peers: peers,
		Extra: map[string]string{"ttft_limit_ms": "60000", "zz_new_tunable": "9"}}) // 任意 key 原样渲染
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	// cart peer 带命名字段 probe = "/health";后端 peer 不带(用 route 默认 /v1/models)
	wantCart := `{ "10.0.0.1", 8071, "cart-glm-0", 1, 180, probe = "/health" },`
	wantBe1 := `{ "10.1.0.1", 8050, "glm-backends-0" },`
	wantBe2 := `{ "10.1.0.2", 8050, "glm-backends-1" },`
	if strings.Contains(conf, `"glm-backends-0", `) { // 后端不该出现任何 probe/priority 尾巴
		t.Errorf("backend peer should stay bare (no probe/priority)\n%s", conf)
	}
	// Extra 任意 key(含 openresty 以后新加的)都渲染,无需改代码
	for _, w := range []string{wantCart, wantBe1, wantBe2, "listen unix:/usr/local/openresty/nginx/sock/glm.sock", "ttft_limit_ms = 60000,", "zz_new_tunable = 9,"} {
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
	conf, err := RenderRoute(RouteData{Route: "glm", Peers: peers})
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	if !strings.Contains(conf, `{ "10.1.0.1", 8050, "glm-backends-0" },`) {
		t.Errorf("single-target render wrong:\n%s", conf)
	}
}

// luaVal:数字/布尔原样,字符串加引号+转义(防注入)。
func TestLuaVal(t *testing.T) {
	cases := []struct{ in, want string }{
		{"60000", "60000"},           // 数字
		{"0.3", "0.3"},               // 小数
		{"true", "true"},             // 布尔
		{"hello", `"hello"`},         // 字符串加引号
		{"1, evil=2", `"1, evil=2"`}, // 含逗号 → 引号包住,注入不了
		{`a"b`, `"a\"b"`},            // 引号转义
	}
	for _, c := range cases {
		if got := luaVal(c.in); got != c.want {
			t.Errorf("luaVal(%q)=%q want %q", c.in, got, c.want)
		}
	}
}
