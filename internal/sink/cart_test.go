package sink

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

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
	// base 为空 = 那个 key 还没内容:从空文档起渲染,只吐 workers(没东西会被覆盖)。
	// 「读不到就别写」的 fail-safe 在 controller 那层按【CM 能否读到】判定,不在这里。
	if _, err := RenderCart("", nil, 0); err != nil {
		t.Errorf("空 base 不该报错: %v", err)
	}
	// per-worker max_load 覆盖
	o, _ := RenderCart("server: {}", []config.Peer{{IP: "1.2.3.4", Port: 80, MaxLoad: 5}}, 20)
	if !strings.Contains(o, "max_load: 5") {
		t.Errorf("per-worker max_load 应生效\n%s", o)
	}
}

// 覆盖层模式:只吐 workers,不需要也不读底稿。
// 覆盖层 key(base 为空):输出只有 workers,不掺任何别的键 —— 掺了会在 CART 合并时盖掉底稿。
func TestRenderCartEmptyBase(t *testing.T) {
	out, err := RenderCart("", []config.Peer{
		{IP: "10.1.0.1", Port: 8050},
		{IP: "10.1.0.2", Port: 8050, MaxLoad: 5},
	}, 20)
	if err != nil {
		t.Fatalf("RenderCart(空 base): %v", err)
	}
	var doc map[string]interface{}
	if err := yaml.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("output is not YAML: %v\n%s", err, out)
	}
	if len(doc) != 1 {
		t.Fatalf("空 base 只该产出 workers 一个键,得到 %v", doc)
	}
	ws, ok := doc["workers"].([]interface{})
	if !ok || len(ws) != 2 {
		t.Fatalf("workers 不对: %#v", doc["workers"])
	}
	w0 := ws[0].(map[string]interface{})
	if w0["url"] != "http://10.1.0.1:8050" || w0["max_load"] != 20 {
		t.Errorf("peer0 应用默认 max_load: %#v", w0)
	}
	if ws[1].(map[string]interface{})["max_load"] != 5 {
		t.Errorf("peer1 应保留自己的 max_load: %#v", ws[1])
	}
}
