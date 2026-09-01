package sink

import (
	"strings"
	"testing"

	slov1 "autoconfig/api/inference/v1alpha1"
	"autoconfig/internal/config"
)

func metric(t string, th float64) slov1.SLOMetric { return slov1.SLOMetric{Type: t, Threshold: th} }

func target(ms ...slov1.SLOMetric) *slov1.SLOTarget {
	return &slov1.SLOTarget{Default: &slov1.SLODefault{Metrics: ms}}
}

// 线上 kimi-k25 那份 CRD 的完整翻译,并真的渲染进 route.tmpl。
// 这条把整个契约钉住:方向换算(ttft 高尾 / otps 低尾)、单位换算(秒 → 毫秒)、
// 以及【Raw 不过 luaVal】—— 走 Extra 会被整个加引号,引擎读到的就不是 table。
func TestRenderSLOMetrics_KimiLive(t *testing.T) {
	m, warns, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{
		ServiceID: "kimi-k25",
		TTFT:      target(metric("p80", 20)), // 80% 请求 TTFT ≤ 20s
		OTPS:      target(metric("p80", 15)), // 80% 请求 OTPS ≥ 15 tok/s
	})
	if err != nil || len(warns) != 0 {
		t.Fatalf("RenderSLOMetrics: err=%v warns=%v", err, warns)
	}
	want := map[string]string{
		"TTFT": `{ { metric = "p80", q = 0.8, threshold = 20000 }, }`, // 秒 → 毫秒
		"TPS":  `{ { metric = "p80", q = 0.2, threshold = 15 }, }`,    // coverage 0.8 → 低尾 q=0.2
	}
	if m.TTFT != want["TTFT"] {
		t.Errorf("ttft_metrics\n got %s\nwant %s", m.TTFT, want["TTFT"])
	}
	if m.TPS != want["TPS"] {
		t.Errorf("tps_metrics\n got %s\nwant %s", m.TPS, want["TPS"])
	}

	out, err := RenderRoute(RouteData{
		Route: "kimi-k2.5",
		Extra: map[string]string{"ttft_limit_ms": "30000"},
		Raw:   WithSLOMetrics(nil, m),
		Peers: []config.Peer{{IP: "10.1.0.1", Port: 8050, Name: "n1"}},
	})
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	if !strings.Contains(out, `ttft_metrics = { { metric = "p80", q = 0.8, threshold = 20000 }, },`) {
		t.Errorf("Raw 应原样输出为 lua table\n%s", out)
	}
	// Raw 若误走 luaVal 会长成 ttft_metrics = "{ { metric = ...",这条就是防它
	if strings.Contains(out, `ttft_metrics = "`) {
		t.Errorf("Raw 被当成字符串加了引号(应绕过 luaVal)\n%s", out)
	}
	// 静态默认值仍然渲染 —— 两个来源并存,CRD 只是在引擎的优先级链里排在它前面
	if !strings.Contains(out, "ttft_limit_ms = 30000,") {
		t.Errorf("nginx.values 的静态值不该被 SLO 挤掉\n%s", out)
	}
}

// otps 的 q 是 1-coverage:p99 → 0.01。引擎要求 0<q<1,不能被 round 成 0。
func TestSLOQuantileDirections(t *testing.T) {
	cases := []struct {
		kind, typ string
		want      float64
	}{
		{"ttft", "p50", 0.5}, {"otps", "p50", 0.5},
		{"ttft", "p95", 0.95}, {"otps", "p95", 0.05},
		{"ttft", "p99", 0.99}, {"otps", "p99", 0.01},
	}
	for _, c := range cases {
		got, hasQ, ok := sloQuantile(c.kind, c.typ)
		if !ok || !hasQ || got != c.want {
			t.Errorf("sloQuantile(%s,%s) = %v,%v,%v; want %v", c.kind, c.typ, got, hasQ, ok, c.want)
		}
		if got <= 0 || got >= 1 {
			t.Errorf("%s/%s 的 q=%v 落在引擎的 0<q<1 之外,会被整份丢弃", c.kind, c.typ, got)
		}
	}
	// avg 没有分位含义 → 不渲染 q(引擎对 avg 也不校验 q)
	m, _, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{TTFT: target(metric("avg", 5))})
	if err != nil {
		t.Fatalf("avg: %v", err)
	}
	if strings.Contains(m.TTFT, "q =") {
		t.Errorf("avg 不该带 q: %s", m.TTFT)
	}
	if !strings.Contains(m.TTFT, "threshold = 5000") {
		t.Errorf("avg 的 ttft 阈值同样要 ×1000: %s", m.TTFT)
	}
}

// 未知 type 必须报错,绝不猜一个默认分位 —— 猜错会让限流悄悄按错误分位跑,没人会发现。
func TestRenderSLOMetrics_Rejects(t *testing.T) {
	if _, _, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{TTFT: target(metric("p42", 1))}); err == nil {
		t.Error("未知 type 应报错")
	}
	if _, _, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{OTPS: target(metric("p80", -1))}); err == nil {
		t.Error("负阈值应报错")
	}
	// 两个维度都没声明 → Empty,调用方据此完全不往 conf 里塞这两项
	m, _, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{ServiceID: "x"})
	if err != nil || !m.Empty() {
		t.Errorf("空 spec 应 Empty,得到 %+v,%v", m, err)
	}
	// 只有 ranges 没有 default → 等同未声明,且要给出 WARN(静默忽略会让人以为 ranges 生效了)
	m, warns, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{
		TTFT: &slov1.SLOTarget{Ranges: []slov1.SLORange{{ContextLengthRangeLow: 0}}},
	})
	if err != nil || !m.Empty() {
		t.Errorf("只有 ranges 应 Empty,得到 %+v,%v", m, err)
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "ranges") {
		t.Errorf("声明了 ranges 必须 WARN,得到 %v", warns)
	}
}

// 多指标:同一维度声明多条时全部渲染(引擎判定是 OR)。
func TestRenderSLOMetrics_MultiMetric(t *testing.T) {
	m, _, err := RenderSLOMetrics(slov1.LLMSLORequirementSpec{
		TTFT: target(metric("p95", 30), metric("avg", 8)),
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	want := `{ { metric = "p95", q = 0.95, threshold = 30000 }, { metric = "avg", threshold = 8000 }, }`
	if m.TTFT != want {
		t.Errorf("\n got %s\nwant %s", m.TTFT, want)
	}
}

// WithSLOMetrics 不改入参 —— 入参可能来自 informer 缓存,就地改会污染它。
func TestWithSLOMetrics_NoMutate(t *testing.T) {
	in := map[string]string{"a": "1"}
	out := WithSLOMetrics(in, SLOMetrics{TTFT: "{}"})
	if len(in) != 1 {
		t.Errorf("入参被改了: %v", in)
	}
	if out["a"] != "1" || out["ttft_metrics"] != "{}" {
		t.Errorf("产出不对: %v", out)
	}
	// 空的维度不该留下一个空 key(引擎会把 ttft_metrics = 当成语法错)
	out = WithSLOMetrics(nil, SLOMetrics{})
	if _, ok := out["ttft_metrics"]; ok {
		t.Errorf("空维度不该产生 key: %v", out)
	}
}

// Extra 与 Raw 出现同名键时,**Raw 必须在后** —— lua 表构造式里后写的赢。
//
// 这条不是形式主义:2026-09-01 在 k8s-cpu-16 上实测过,用户在 nginx.values 里手写
// ttft_metrics 时,conf 里会真的出现两行同名键(Extra 那份被 luaVal 加了引号变成字符串)。
// 顺序对 → CRD 的 table 生效;顺序反了 → 引擎收到字符串 → validate_metrics 整份丢弃 →
// **静默降级成静态阈值**,而 /_ttft_status 的 source 看不出任何异常。
//
// 也就是说这个正确性此前**只由 route.tmpl 里两个 range 块的先后决定,没有任何测试保护**,
// 有人重排模板就会翻车。这条测试就是那道保护。
func TestRenderRoute_RawWinsOverExtra(t *testing.T) {
	out, err := RenderRoute(RouteData{
		Route: "r",
		Extra: map[string]string{"ttft_metrics": "BOGUS"}, // 用户手写 → 走 luaVal → 加引号
		Raw:   map[string]string{"ttft_metrics": `{ { metric = "p95", q = 0.95, threshold = 35000 }, }`},
		Peers: []config.Peer{{IP: "10.1.0.1", Port: 8050, Name: "n1"}},
	})
	if err != nil {
		t.Fatalf("RenderRoute: %v", err)
	}
	quoted := strings.Index(out, `ttft_metrics = "BOGUS"`)
	table := strings.Index(out, `ttft_metrics = { { metric = "p95"`)
	if quoted < 0 || table < 0 {
		t.Fatalf("两份都该出现在 conf 里(quoted=%d table=%d)\n%s", quoted, table, out)
	}
	if table < quoted {
		t.Errorf("Raw 必须排在 Extra 之后,否则 CRD 的 table 会被手写字符串覆盖 → "+
			"validate_metrics 丢弃 → 静默降级成静态阈值\n%s", out)
	}

	// 更直接的断言:最后一次出现的 ttft_metrics 必须是**未加引号的 table** ——
	// 这正是 lua 实际会用的那一份。上面的下标比较若因模板改写而失真,这条仍能兜住。
	last := strings.LastIndex(out, "ttft_metrics =")
	if !strings.HasPrefix(out[last:], `ttft_metrics = { `) {
		t.Errorf("生效的(最后一份)ttft_metrics 不是 table:%.60q", out[last:])
	}
}
