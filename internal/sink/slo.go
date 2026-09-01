package sink

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	slov1 "autoconfig/api/inference/v1alpha1"
)

// ── LLMSLORequirement → openresty 的 ttft_metrics / tps_metrics ────────────────
//
// 产出是一段 **lua 字面量**,渲染进 session_route_<route>.conf 的 register_route 返回表:
//
//	ttft_metrics = { { metric = "p80", q = 0.8, threshold = 20000 }, },
//	tps_metrics  = { { metric = "p80", q = 0.2, threshold = 15 }, },
//
// 与 ttft_limit_ms / tps_limit_tps 等其它调优项**走同一条通道**(operator 重写 conf →
// reload sidecar SIGHUP)。曾经考虑过给 SLO 单开一条「独立 ConfigMap + lua loader timer」的
// 免 reload 通道,核对生产数据后放弃:llm-route 的 openresty 因 peer churn **每天已经
// reload 几十次**(实测 24h 内 34 次),而 LLMSLORequirement 是人手写的、改动是周级 ——
// 为了躲一个本来就在频繁发生的 reload 去维护第二套配置通道,不划算。真正的免 reload
// 应急口子是 /_ttft_limit / /_tps_limit(写 shared dict,优先级还压过 CRD)。
//
// 引擎侧在 register_route 里用 util.validate_metrics 再校验一遍(每 reload 一次):
// 任何一条不合法就整份丢弃、回落静态阈值。所以这里渲染出的坏数据不会让路由挂掉,
// 但也就**静默降级**了 —— 故本函数对不认识的输入一律报错,不猜。

// SLOMetrics 是翻译结果:两段 lua 字面量,空串表示该维度不覆盖(引擎回落静态)。
type SLOMetrics struct {
	TTFT string // ttft_metrics 的值;"" = 不渲染这一项
	TPS  string // tps_metrics 的值
}

// Empty 表示 CRD 没声明任何我们用得上的维度 → 调用方不该往 conf 里塞这两项。
func (m SLOMetrics) Empty() bool { return m.TTFT == "" && m.TPS == "" }

// sloQuantile 把 CRD 的覆盖率类型换算成引擎的取分位系数。
//
//	ttft "p80" = 「80% 请求 TTFT ≤ 阈值」→ 违约看**高尾 P80** → q = 0.80
//	otps "p80" = 「80% 请求 OTPS ≥ 阈值」→ 违约看**低尾 P20** → q = 1 - 0.80 = 0.20
//
// 方向相反(TTFT 越大越坏、OTPS 越大越好)。**换算只在这里做一次** —— 引擎只认
// 「取第 q 分位」这一个原语、不做方向判断,单一真相点,免得两边各写一遍导致某边写反。
//
// avg 无分位含义,返回 ok=true 但 hasQ=false(引擎对 avg 也不校验 q)。
// 未知 type 返回 ok=false —— **整份拒绝**,不猜、不给默认值:猜错会让某条 route 的
// 限流悄悄按错误的分位跑,而没有任何人会发现。
func sloQuantile(kind, typ string) (q float64, hasQ, ok bool) {
	if typ == "avg" {
		return 0, false, true
	}
	var cov float64
	switch typ {
	case "p50":
		cov = 0.50
	case "p80":
		cov = 0.80
	case "p90":
		cov = 0.90
	case "p95":
		cov = 0.95
	case "p99":
		cov = 0.99
	default:
		return 0, false, false
	}
	if kind == "otps" {
		cov = 1 - cov
	}
	// round 到 3 位小数:otps 侧的 1-0.99 = 0.010000000000000009,直接渲染会在 conf 里
	// 留一串噪声数字。引擎只要 0<q<1,值本身不敏感;p99 → 0.01 仍在区间内。
	return math.Round(cov*1000) / 1000, true, true
}

// luaNum 渲染 lua 数字字面量,去掉无意义的尾随零(20000 而不是 20000.000000)。
// 用 'f' 而非 'g':'g' 对大数会出科学计数法(1e+06),lua 虽然认,但 conf 里难读、
// 也和旁边手写的 ttft_limit_ms 对不上眼。
func luaNum(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// renderMetrics 翻译一个维度(ttft / otps)成 lua 字面量。
// CRD 未声明该维度、或声明了但没有 default → 返回 ""(= 不覆盖,引擎回落静态)。
func renderMetrics(kind string, t *slov1.SLOTarget) (string, error) {
	if t == nil || t.Default == nil || len(t.Default.Metrics) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.WriteString("{ ")
	for i, m := range t.Default.Metrics {
		q, hasQ, ok := sloQuantile(kind, m.Type)
		if !ok {
			return "", fmt.Errorf("%s.default.metrics[%d]: 未知的 type %q", kind, i, m.Type)
		}
		if m.Threshold < 0 {
			return "", fmt.Errorf("%s.default.metrics[%d]: threshold 不能为负", kind, i)
		}
		th := m.Threshold
		if kind == "ttft" {
			th *= 1000 // CRD 的 ttft.threshold 单位是**秒**,引擎内部是毫秒
		}
		b.WriteString(`{ metric = "` + m.Type + `", `)
		if hasQ {
			b.WriteString("q = " + luaNum(q) + ", ")
		}
		b.WriteString("threshold = " + luaNum(th) + " }, ")
	}
	b.WriteString("}")
	return b.String(), nil
}

// RenderSLOMetrics 把一个 LLMSLORequirement 的 spec 翻译成两段 lua 字面量。
// 第二个返回值是需要打 WARN 的提示(如声明了 ranges 但本期忽略);nil 表示没有。
//
// ranges[] **本期不实现**:判定 per-range 需要 access 阶段的 context length(那时还没有
// usage),而且 AIMD 的并发是整池一个、映射不到 per-range。这里显式忽略并回一条提示,
// 由调用方打日志 —— 静默忽略会让人以为 ranges 已经生效。
func RenderSLOMetrics(spec slov1.LLMSLORequirementSpec) (SLOMetrics, []string, error) {
	var out SLOMetrics
	var err error
	if out.TTFT, err = renderMetrics("ttft", spec.TTFT); err != nil {
		return SLOMetrics{}, nil, err
	}
	if out.TPS, err = renderMetrics("otps", spec.OTPS); err != nil {
		return SLOMetrics{}, nil, err
	}
	var warns []string
	for _, x := range []struct {
		name string
		t    *slov1.SLOTarget
	}{{"ttft", spec.TTFT}, {"otps", spec.OTPS}} {
		if x.t != nil && len(x.t.Ranges) > 0 {
			warns = append(warns, fmt.Sprintf(
				"%s 声明了 %d 条 ranges,本版本不支持按 contextLength 分段,已忽略(只用 default)",
				x.name, len(x.t.Ranges)))
		}
	}
	return out, warns, nil
}

// WithSLOMetrics 把翻译结果并进 route.tmpl 的 Raw 表(原样输出的 lua 片段)。
// 返回新 map,不改入参 —— 入参可能来自 informer 缓存里的对象,就地改会污染它。
func WithSLOMetrics(raw map[string]string, m SLOMetrics) map[string]string {
	out := make(map[string]string, len(raw)+2)
	for k, v := range raw {
		out[k] = v
	}
	if m.TTFT != "" {
		out["ttft_metrics"] = m.TTFT
	}
	if m.TPS != "" {
		out["tps_metrics"] = m.TPS
	}
	return out
}
