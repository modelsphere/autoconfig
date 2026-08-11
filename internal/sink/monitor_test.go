package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

func TestRenderMonitor(t *testing.T) {
	backends := []config.Peer{{IP: "10.1.0.1", Port: 8050}, {IP: "10.1.0.2", Port: 8050}}
	nginx := []config.Peer{{IP: "10.2.0.1", Port: 18080}}
	router := []config.Peer{{IP: "10.3.0.1", Port: 8071}}
	out := RenderMonitor("kimi-k2.6", "kimi-k2.6", "H100", "openresty", "kimi-k2.6", backends, nginx, router)
	for _, w := range []string{
		"service: kimi-k2.6-0 | http://10.1.0.1:8050 | kimi-k2.6 | H100",
		"service: kimi-k2.6-1 | http://10.1.0.2:8050 | kimi-k2.6 | H100",
		"nginx: openresty-nginx-0 | http://10.2.0.1:18080/kimi-k2.6",
		"router: kimi-k2.6-router-0 | http://10.3.0.1:8071/workers",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q\n%s", w, out)
		}
	}
	// model 省略时用 mrName;nginx/router 为空则不出对应行
	only := RenderMonitor("m1", "", "", "", "m1", []config.Peer{{IP: "1.2.3.4", Port: 80}}, nil, nil)
	if !strings.Contains(only, "service: m1-0 | http://1.2.3.4:80 | m1 | ") {
		t.Error("empty model should fall back to mrName")
	}
	if strings.Contains(only, "nginx:") || strings.Contains(only, "router:") {
		t.Errorf("empty nginx/router should emit no lines\n%s", only)
	}
}

// 混布:各 peer 用自己节点的 GPU 型号(旧实现整组套第一个,除首行外全错)。
func TestRenderMonitorPerPeerGPU(t *testing.T) {
	backends := []config.Peer{
		{IP: "10.1.0.1", Port: 8050, GPU: "H100"},
		{IP: "10.1.0.2", Port: 8050, GPU: "H800"},
		{IP: "10.1.0.3", Port: 8050, GPU: ""}, // 推不出 → 留空
	}
	got := RenderMonitor("kimi", "kimi-k2.6", "", "", "", backends, nil, nil)
	for _, want := range []string{
		"service: kimi-0 | http://10.1.0.1:8050 | kimi-k2.6 | H100\n",
		"service: kimi-1 | http://10.1.0.2:8050 | kimi-k2.6 | H800\n",
		"service: kimi-2 | http://10.1.0.3:8050 | kimi-k2.6 | \n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少行 %q\n实际:\n%s", want, got)
		}
	}
}

// CRD 显式配 gpuType → 整条 route 覆盖,忽略各 peer 自动推导值。
func TestRenderMonitorExplicitGPUOverrides(t *testing.T) {
	backends := []config.Peer{
		{IP: "10.1.0.1", Port: 8050, GPU: "H100"},
		{IP: "10.1.0.2", Port: 8050, GPU: "H800"},
	}
	got := RenderMonitor("kimi", "kimi-k2.6", "A100", "", "", backends, nil, nil)
	if strings.Contains(got, "H100") || strings.Contains(got, "H800") {
		t.Errorf("显式 gpuType 应覆盖逐 peer 值,实际:\n%s", got)
	}
	if strings.Count(got, "| A100\n") != 2 {
		t.Errorf("两行都应是 A100,实际:\n%s", got)
	}
}
