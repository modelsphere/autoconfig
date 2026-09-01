package sink

import (
	"encoding/json"
	"fmt"
	"math"

	slov1 "autoconfig/api/inference/v1alpha1"
)

// ── openresty 的 slo.json 契约 ────────────────────────────────────────────────
//
//	{ "version": 17,
//	  "routes": { "<route>": { "__default__": {
//	      "ttft": { "default": { "metrics": [ {"metric":"p80","q":0.8,"threshold_ms":20000} ] }, "ranges": [] },
//	      "otps": { "default": { "metrics": [ {"metric":"p80","q":0.2,"threshold_tps":30}  ] }, "ranges": [] }
//	  } } } }
//
// 结构**镜像 CRD 的 ttft.default / ttft.ranges**,翻译是逐字段搬运 —— 将来支持 ranges 只是把
// 数组填上,不改 schema、不改引擎的 EWMA key 布局。
//
// 全部 route 装在**一个 ConfigMap 的一个 key**里,不是一 route 一文件。三个理由:
//  1. OpenResty 的 LuaJIT **没有 lfs**,列不了目录 —— 扫目录只能 io.popen("ls"),在 ngx.timer 里 fork 不可接受。
//     单文件让 loader 只需 io.open 一个固定路径。
//  2. ConfigMap 的 ..data symlink 是**整体原子切换** → 所有 route 拿到同一份一致快照,不会半新半旧。
//  3. 新增 route 只是往 json 里加一项,不用改 Deployment(多 ConfigMap 方案要加 volume → 重启 pod)。
//
// ⚠️ 这个 ConfigMap **绝不能挂进 openresty 的 /watch** —— reload sidecar 盯着 /watch,
// 挂进去会让每次 SLO 变更都连带触发一次 openresty reload,免 reload 的设计就白做了。

// SLOModelDefault 是 model 层的恒定键。生产无 peers_by_model 路由(每个 route 就是一个模型),
// 这一层目前恒为 __default__;留着是零成本对齐引擎已有的 per-model key 前缀。
const SLOModelDefault = "__default__"

// SLOKey 是 slo.json 在 ConfigMap 里的 key(= 挂进容器后的文件名)。
const SLOKey = "slo.json"

type sloMetricOut struct {
	Metric string `json:"metric"`
	// Q 是**引擎直接用的取分位系数**,不是 CRD 的覆盖率。方向换算只在这里做一次
	// (见 sloQuantile);引擎不做方向判断 —— 单一真相点,免得两边各写一遍导致某边写反。
	// avg 没有分位含义,omitempty 让它不出现(引擎对 avg 也不校验 q)。
	Q            float64  `json:"q,omitempty"`
	ThresholdMS  *float64 `json:"threshold_ms,omitempty"`
	ThresholdTPS *float64 `json:"threshold_tps,omitempty"`
}

type sloDefaultOut struct {
	Metrics []sloMetricOut `json:"metrics"`
}

type sloTargetOut struct {
	Default *sloDefaultOut `json:"default,omitempty"`
	// Ranges 恒为空数组(本期不实现)。**显式输出 `[]` 而不是省略**:让运维一眼看出
	// "这个字段存在、只是没启用",而不是以为 operator 漏了翻译。
	Ranges []struct{} `json:"ranges"`
}

// SLOModelOut 是 slo.json 里 routes.<route>.<model> 那一层。
type SLOModelOut struct {
	TTFT *sloTargetOut `json:"ttft,omitempty"`
	OTPS *sloTargetOut `json:"otps,omitempty"`
}

// SLODoc 是整份 slo.json。Routes 用 map → encoding/json 按 key 排序输出,渲染确定性。
type SLODoc struct {
	Version int                               `json:"version"`
	Routes  map[string]map[string]SLOModelOut `json:"routes"`
}

// sloQuantile 把 CRD 的覆盖率类型换算成引擎的取分位系数。
//
//	ttft "p80" = 「80% 请求 ≤ 阈值」→ 违约看**高尾 P80** → q = 0.80
//	otps "p80" = 「80% 请求 ≥ 阈值」→ 违约看**低尾 P20** → q = 1 - 0.80 = 0.20
//
// avg 无分位含义,返回 0(输出时被 omitempty 吃掉)。
// 未知 type 返回 ok=false —— **整份拒绝**,不猜、不给默认值:猜错会让某个 route
// 的限流悄悄按错误的分位跑,而没有任何人会发现。
func sloQuantile(kind, typ string) (float64, bool) {
	if typ == "avg" {
		return 0, true
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
		return 0, false
	}
	if kind == "otps" {
		cov = 1 - cov
	}
	// round 到 3 位小数:p99 的 otps 侧是 1-0.99 = 0.010000000000000009,
	// 直接下发会让 json 里出现一串噪声数字(引擎只要 0<q<1,值本身不敏感)。
	return math.Round(cov*1000) / 1000, true
}

// renderTarget 翻译一个维度(ttft / otps)。CRD 未声明该维度、或声明了但没有 default
// → 返回 nil = 该维度不覆盖,引擎回落静态配置。
func renderTarget(kind string, t *slov1.SLOTarget) (*sloTargetOut, error) {
	if t == nil || t.Default == nil || len(t.Default.Metrics) == 0 {
		return nil, nil
	}
	out := make([]sloMetricOut, 0, len(t.Default.Metrics))
	for i, m := range t.Default.Metrics {
		q, ok := sloQuantile(kind, m.Type)
		if !ok {
			return nil, fmt.Errorf("%s.default.metrics[%d]: 未知的 type %q", kind, i, m.Type)
		}
		if m.Threshold < 0 {
			return nil, fmt.Errorf("%s.default.metrics[%d]: threshold 不能为负", kind, i)
		}
		mo := sloMetricOut{Metric: m.Type, Q: q}
		if kind == "ttft" {
			// CRD 的 ttft.threshold 单位是**秒**,引擎内部是毫秒。
			v := m.Threshold * 1000
			mo.ThresholdMS = &v
		} else {
			v := m.Threshold
			mo.ThresholdTPS = &v
		}
		out = append(out, mo)
	}
	return &sloTargetOut{Default: &sloDefaultOut{Metrics: out}, Ranges: []struct{}{}}, nil
}

// RenderSLO 把一个 LLMSLORequirement 的 spec 翻译成该 route 在 slo.json 里的节点。
// 两个维度都没声明 → 返回 nil(调用方应把这条 route 从 slo.json 里摘掉,而不是写个空节点)。
func RenderSLO(spec slov1.LLMSLORequirementSpec) (map[string]SLOModelOut, error) {
	ttft, err := renderTarget("ttft", spec.TTFT)
	if err != nil {
		return nil, err
	}
	otps, err := renderTarget("otps", spec.OTPS)
	if err != nil {
		return nil, err
	}
	if ttft == nil && otps == nil {
		return nil, nil
	}
	return map[string]SLOModelOut{SLOModelDefault: {TTFT: ttft, OTPS: otps}}, nil
}

// MergeSLO 把一条 route 的节点并进现有 slo.json,返回新内容与是否有变化。
//
// **只碰自己那条 route**,其余 route 原样保留 —— 多个 ModelRoute 的 reconcile 并发写同一个
// ConfigMap,靠 RetryOnConflict + 这里的窄粒度合并做到互不覆盖。
// node 为 nil = 把该 route 摘掉(CRD 被删 / 两个维度都没声明)。
//
// existing 解析失败时**不报错、当空文档从头建**:那份内容不是我们能修的(手工改坏 / 格式换代),
// 报错只会让 reconcile 永久卡住,而重建至少能自愈 —— 代价是丢掉别的 route 的节点,
// 但它们各自的 reconcile 会在 10s resync 内把自己写回来。
//
// version 在**有变化时**才 +1:引擎日志打印它,用来确认"线上生效的是哪一版";
// 无变化不 bump 可以让 ConfigMap 不被写、也就不产生多余的 kubelet 同步。
func MergeSLO(existing, route string, node map[string]SLOModelOut) (string, bool, error) {
	doc := SLODoc{Routes: map[string]map[string]SLOModelOut{}}
	if existing != "" {
		var prev SLODoc
		if err := json.Unmarshal([]byte(existing), &prev); err == nil && prev.Routes != nil {
			doc = prev
		}
	}
	if doc.Routes == nil {
		doc.Routes = map[string]map[string]SLOModelOut{}
	}

	// 比对时用序列化后的字节,而不是 reflect.DeepEqual:后者对 *float64 比的是指针。
	before, _ := json.Marshal(doc.Routes[route])
	_, had := doc.Routes[route]
	if node == nil {
		if !had {
			return existing, false, nil
		}
		delete(doc.Routes, route)
	} else {
		after, _ := json.Marshal(node)
		if had && string(before) == string(after) {
			return existing, false, nil
		}
		doc.Routes[route] = node
	}
	doc.Version++

	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", false, fmt.Errorf("marshal slo.json: %w", err)
	}
	return string(out) + "\n", true, nil
}
