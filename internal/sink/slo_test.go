package sink

import (
	"encoding/json"
	"strings"
	"testing"

	slov1 "autoconfig/api/inference/v1alpha1"
)

func metric(t string, th float64) slov1.SLOMetric { return slov1.SLOMetric{Type: t, Threshold: th} }

func target(ms ...slov1.SLOMetric) *slov1.SLOTarget {
	return &slov1.SLOTarget{Default: &slov1.SLODefault{Metrics: ms}}
}

// 线上 kimi-k25 那份 CRD 的完整翻译。这条把整个契约钉住:
// 方向换算(ttft 高尾 / otps 低尾)、单位换算(秒 → 毫秒)、字段名、ranges 空数组。
func TestRenderSLO_KimiLive(t *testing.T) {
	node, err := RenderSLO(slov1.LLMSLORequirementSpec{
		ServiceID: "kimi-k25",
		TTFT:      target(metric("p80", 20)), // 80% 请求 TTFT ≤ 20s
		OTPS:      target(metric("p80", 15)), // 80% 请求 OTPS ≥ 15 tok/s
	})
	if err != nil {
		t.Fatalf("RenderSLO: %v", err)
	}
	out, changed, err := MergeSLO("", "kimi-k2.5", node)
	if err != nil || !changed {
		t.Fatalf("MergeSLO: err=%v changed=%v", err, changed)
	}
	for _, want := range []string{
		`"metric": "p80"`,
		`"q": 0.8`,              // ttft: coverage 0.8 → 取高尾 P80
		`"q": 0.2`,              // otps: coverage 0.8 → 取低尾 P20(1-0.8)
		`"threshold_ms": 20000`, // 秒 → 毫秒
		`"threshold_tps": 15`,
		`"ranges": []`, // 显式空数组,不省略
		`"__default__"`,
		`"version": 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("缺少 %s\n%s", want, out)
		}
	}
	// ttft 节点不该出现 threshold_tps,反之亦然(字段串了引擎会当"缺阈值"整份拒绝)
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("产出不是合法 JSON: %v", err)
	}
}

// otps 的 q 是 1-coverage:p99 → 0.01。引擎要求 0<q<1,不能被 round 成 0。
func TestRenderSLO_QuantileDirections(t *testing.T) {
	cases := []struct {
		kind, typ string
		want      float64
	}{
		{"ttft", "p50", 0.5}, {"otps", "p50", 0.5},
		{"ttft", "p95", 0.95}, {"otps", "p95", 0.05},
		{"ttft", "p99", 0.99}, {"otps", "p99", 0.01},
	}
	for _, c := range cases {
		got, ok := sloQuantile(c.kind, c.typ)
		if !ok || got != c.want {
			t.Errorf("sloQuantile(%s,%s) = %v,%v; want %v", c.kind, c.typ, got, ok, c.want)
		}
		if got <= 0 || got >= 1 {
			t.Errorf("%s/%s 的 q=%v 落在引擎的 0<q<1 之外,会被整份拒绝", c.kind, c.typ, got)
		}
	}
	// avg 没有分位含义 → q=0,输出时被 omitempty 吃掉(引擎对 avg 也不校验 q)
	node, err := RenderSLO(slov1.LLMSLORequirementSpec{TTFT: target(metric("avg", 5))})
	if err != nil {
		t.Fatalf("avg: %v", err)
	}
	out, _, _ := MergeSLO("", "r", node)
	if strings.Contains(out, `"q"`) {
		t.Errorf("avg 不该带 q\n%s", out)
	}
}

// 未知 type 必须报错,绝不猜一个默认分位 —— 猜错会让限流悄悄按错误分位跑,没人会发现。
func TestRenderSLO_UnknownTypeRejected(t *testing.T) {
	if _, err := RenderSLO(slov1.LLMSLORequirementSpec{TTFT: target(metric("p42", 1))}); err == nil {
		t.Error("未知 type 应报错")
	}
	// 两个维度都没声明 → nil(调用方据此把 route 摘掉,而不是写个空节点)
	node, err := RenderSLO(slov1.LLMSLORequirementSpec{ServiceID: "x"})
	if err != nil || node != nil {
		t.Errorf("空 spec 应返回 nil,得到 %v,%v", node, err)
	}
}

// 合并只碰自己那条 route —— 这是多个 ModelRoute 并发写同一个 key 的正确性前提。
func TestMergeSLO_IsolatesOtherRoutes(t *testing.T) {
	a, _, _ := MergeSLO("", "route-a", mustRender(t, target(metric("p80", 10)), nil))
	b, changed, err := MergeSLO(a, "route-b", mustRender(t, target(metric("p90", 30)), nil))
	if err != nil || !changed {
		t.Fatalf("merge b: %v %v", err, changed)
	}
	var doc SLODoc
	if err := json.Unmarshal([]byte(b), &doc); err != nil {
		t.Fatalf("%v", err)
	}
	if len(doc.Routes) != 2 || doc.Routes["route-a"] == nil || doc.Routes["route-b"] == nil {
		t.Fatalf("两条 route 都应在:%s", b)
	}
	if doc.Version != 2 {
		t.Errorf("version 应逐次 +1,得到 %d", doc.Version)
	}

	// 无变化不写(避免多余的 ConfigMap churn / kubelet 同步)
	if _, changed, _ := MergeSLO(b, "route-b", mustRender(t, target(metric("p90", 30)), nil)); changed {
		t.Error("同样的内容不应报告 changed")
	}
	// node=nil = 摘掉自己那条,别的 route 保留
	c, changed, _ := MergeSLO(b, "route-b", nil)
	if !changed {
		t.Fatal("删除应报告 changed")
	}
	// ⚠️ 必须用**新变量**:json.Unmarshal 往已有的非 nil map 里是【合并】而非替换,
	// 复用上面的 doc 会让被删掉的 route-b 看起来还在(假失败)。
	var after SLODoc
	if err := json.Unmarshal([]byte(c), &after); err != nil {
		t.Fatalf("%v", err)
	}
	doc = after
	if _, ok := doc.Routes["route-b"]; ok {
		t.Errorf("route-b 应被摘掉:%s", c)
	}
	if _, ok := doc.Routes["route-a"]; !ok {
		t.Errorf("route-a 不该被连坐删掉:%s", c)
	}
	// 摘一条本来就不存在的 route → 无变化(否则每轮 reconcile 都会 bump version 白写)
	if _, changed, _ := MergeSLO(c, "never-existed", nil); changed {
		t.Error("摘不存在的 route 不应报告 changed")
	}
}

// 现有内容坏掉时从头建,而不是报错卡死 reconcile。
func TestMergeSLO_RecoversFromGarbage(t *testing.T) {
	out, changed, err := MergeSLO("{ 这不是 json", "r", mustRender(t, target(metric("p80", 1)), nil))
	if err != nil || !changed {
		t.Fatalf("坏内容应能重建,得到 err=%v changed=%v", err, changed)
	}
	if !strings.Contains(out, `"r"`) {
		t.Errorf("重建后应含本 route:%s", out)
	}
}

func mustRender(t *testing.T, ttft, otps *slov1.SLOTarget) map[string]SLOModelOut {
	t.Helper()
	n, err := RenderSLO(slov1.LLMSLORequirementSpec{TTFT: ttft, OTPS: otps})
	if err != nil {
		t.Fatalf("RenderSLO: %v", err)
	}
	return n
}
