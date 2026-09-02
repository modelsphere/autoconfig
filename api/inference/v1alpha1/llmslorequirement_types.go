// Package v1alpha1 是 LLMSLORequirement(inference.x-k8s.io/v1alpha1)的**只读**最小类型。
//
// 这个 CRD **不归 autoconfig 所有** —— 它由别的组件安装和写入(autoscaler 消费
// priority / minimumDeployment / maximumDeployment;我们只消费 ttft / otps)。
// 所以这里:
//   - 只声明我们读的字段。CRD 后续加字段不会破坏这里(json 解码忽略未知字段),
//     也不会因为我们少声明就把它们从对象里抹掉 —— 我们从不写回这个 CRD。
//   - **刻意不打 +kubebuilder:object:root 标记**:打了 controller-gen 会把它当成我们自己的 API
//     去生成一份 CRD yaml(实测产出 config/crd/bases/_.yaml —— group/kind 全空的垃圾文件)。
//     runtime.Object 接口由下面手写的 DeepCopyObject 满足,不需要 codegen。
//   - 用 typed 而非 unstructured:字段路径写错在编译期就炸,unstructured 只会在运行时静默拿到零值。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is inference.x-k8s.io/v1alpha1.
	GroupVersion = schema.GroupVersion{Group: "inference.x-k8s.io", Version: "v1alpha1"}
	// SchemeBuilder registers the read-only types.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&LLMSLORequirement{}, &LLMSLORequirementList{})
}

// SLOMetric 是一条指标要求。
//
// ⚠️ **Type 是 SLO 覆盖率,不是「取第 NN 百分位」** —— 已与 CRD 作者对齐:
//   - ttft {p80, 20}  读作「80% 的请求 TTFT ≤ 20 秒」→ 引擎要盯分布的**高尾 P80**  → q = 0.8
//   - otps {p80, 30}  读作「80% 的请求 OTPS ≥ 30 tok/s」→ 引擎要盯**低尾 P20** → q = 0.2
//
// 两个方向相反(TTFT 越大越坏、OTPS 越大越好),换算见 sink.RenderSLO。
type SLOMetric struct {
	// Type: avg | p50 | p80 | p90 | p95 | p99
	Type string `json:"type"`
	// Threshold: ttft 的单位是**秒**(引擎内部用毫秒,下发时 ×1000);otps 是 tok/s。
	Threshold float64 `json:"threshold"`
}

// SLODefault 是不命中任何 range 时的兜底指标集。
type SLODefault struct {
	Metrics []SLOMetric `json:"metrics,omitempty"`
}

// SLORange 是按 context length 分段的指标集。
// **autoconfig 原样搬运、openresty 本期忽略**(判定 range 需要 access 阶段的 context length,
// 而且 AIMD 的并发是整池一个、映射不到 per-range)。留着是为了将来启用时不必改 schema。
type SLORange struct {
	ContextLengthRangeLow  int         `json:"contextLengthRangeLow"`
	ContextLengthRangeHigh int         `json:"contextLengthRangeHigh,omitempty"`
	Metrics                []SLOMetric `json:"metrics,omitempty"`
}

// SLOTarget 是一个维度(ttft 或 otps)的完整要求。
type SLOTarget struct {
	Default *SLODefault `json:"default,omitempty"`
	Ranges  []SLORange  `json:"ranges,omitempty"`
}

// LLMSLORequirementSpec 只声明 autoconfig 读的字段。
// priority / minimumDeployment / maximumDeployment 归 autoscaler,路由层不读,故不在此声明。
type LLMSLORequirementSpec struct {
	// ServiceID 指向目标服务。**autoconfig 不用它做关联** —— ModelRoute 通过 spec.slo.name
	// 按 metadata.name 显式引用本对象(见 controller 的 sloMetricsFor)。留着只是为了忠实
	// 描述这个 CRD 的形状,以及让 kubectl 输出/调试时看得到它。
	ServiceID string     `json:"serviceId"`
	TTFT      *SLOTarget `json:"ttft,omitempty"`
	OTPS      *SLOTarget `json:"otps,omitempty"`
}

// LLMSLORequirement 是一个服务的 SLO 声明(外部 CRD,autoconfig 只读)。
type LLMSLORequirement struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec LLMSLORequirementSpec `json:"spec,omitempty"`
}

// LLMSLORequirementList is a list of LLMSLORequirement.
type LLMSLORequirementList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LLMSLORequirement `json:"items"`
}

// ── DeepCopy(手写:本包不进 controller-gen 的 object 生成范围,见包注释)──────────

func (in *SLOMetric) DeepCopyInto(out *SLOMetric) { *out = *in }

func (in *SLODefault) DeepCopyInto(out *SLODefault) {
	*out = *in
	if in.Metrics != nil {
		out.Metrics = make([]SLOMetric, len(in.Metrics))
		copy(out.Metrics, in.Metrics)
	}
}

func (in *SLORange) DeepCopyInto(out *SLORange) {
	*out = *in
	if in.Metrics != nil {
		out.Metrics = make([]SLOMetric, len(in.Metrics))
		copy(out.Metrics, in.Metrics)
	}
}

func (in *SLOTarget) DeepCopyInto(out *SLOTarget) {
	*out = *in
	if in.Default != nil {
		out.Default = new(SLODefault)
		in.Default.DeepCopyInto(out.Default)
	}
	if in.Ranges != nil {
		out.Ranges = make([]SLORange, len(in.Ranges))
		for i := range in.Ranges {
			in.Ranges[i].DeepCopyInto(&out.Ranges[i])
		}
	}
}

func (in *LLMSLORequirementSpec) DeepCopyInto(out *LLMSLORequirementSpec) {
	*out = *in
	if in.TTFT != nil {
		out.TTFT = new(SLOTarget)
		in.TTFT.DeepCopyInto(out.TTFT)
	}
	if in.OTPS != nil {
		out.OTPS = new(SLOTarget)
		in.OTPS.DeepCopyInto(out.OTPS)
	}
}

func (in *LLMSLORequirement) DeepCopyInto(out *LLMSLORequirement) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

func (in *LLMSLORequirement) DeepCopy() *LLMSLORequirement {
	if in == nil {
		return nil
	}
	out := new(LLMSLORequirement)
	in.DeepCopyInto(out)
	return out
}

func (in *LLMSLORequirement) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *LLMSLORequirementList) DeepCopyInto(out *LLMSLORequirementList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]LLMSLORequirement, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *LLMSLORequirementList) DeepCopy() *LLMSLORequirementList {
	if in == nil {
		return nil
	}
	out := new(LLMSLORequirementList)
	in.DeepCopyInto(out)
	return out
}

func (in *LLMSLORequirementList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
