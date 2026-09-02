package sink

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"autoconfig/internal/config"
)

func videoData(peers []config.Peer, extra map[string]string) RouteData {
	return RouteData{Route: "minimax-h3", ModelType: ModelTypeVideo, Peers: peers, Extra: extra}
}

// video 路由必须是纯反向代理:不能出现 lua 路由引擎那套(register_route / dict / 引擎 include),
// 否则请求体会被引擎解析、按 token 速率限流,而视频请求根本没有这些语义。
func TestRenderRoute_VideoIsPlainProxy(t *testing.T) {
	got, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080, Name: "backend-0"}}, nil))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, forbidden := range []string{"register_route", "lua_shared_dict", "router_locations.inc", "set_by_lua_block"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("video 路由不该包含 LLM 引擎的 %q:\n%s", forbidden, got)
		}
	}
	for _, want := range []string{
		"listen unix:/usr/local/openresty/nginx/sock/minimax-h3.sock",
		"upstream backend_minimax-h3",
		"server 10.0.0.1:8080",
		"proxy_pass http://backend_minimax-h3",
		"proxy_buffering off",    // 大视频边收边发
		"proxy_set_header Range", // 断点续传
		"X-Forwarded-Host  $fwdhost_minimax_h3",
		"X-Forwarded-Proto $fwdproto_minimax_h3",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("video 路由缺少 %q:\n%s", want, got)
		}
	}
}

// 默认值:请求体 64m(I2V 传 base64 图)、超时按小时(一条片子 1~3 分钟 + 大文件下载)。
func TestRenderRoute_VideoDefaults(t *testing.T) {
	got, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	for _, want := range []string{"client_max_body_size 64m;", "proxy_read_timeout    3600s;", "proxy_connect_timeout 10s;"} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少默认值 %q:\n%s", want, got)
		}
	}
}

func TestRenderRoute_VideoExtraOverridesDefaults(t *testing.T) {
	got, _ := RenderRoute(videoData(
		[]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"max_body_size": "128m", "proxy_timeout": "600s", "connect_timeout": "3s"},
	))
	for _, want := range []string{"client_max_body_size 128m;", "proxy_read_timeout    600s;", "proxy_connect_timeout 3s;"} {
		if !strings.Contains(got, want) {
			t.Errorf("覆盖值未生效 %q:\n%s", want, got)
		}
	}
}

// 低优先级的那组(backend-svc 这类 VIP 静态兜底)要渲染成 nginx 的 backup:
// 平时不接流量,pod-IP 那层全挂了才顶上 —— 与 LLM 引擎里 priority 的语义对齐。
func TestRenderRoute_VideoLowerPriorityBecomesBackup(t *testing.T) {
	got, _ := RenderRoute(videoData([]config.Peer{
		{IP: "10.0.0.1", Port: 8080, Name: "backend-0", Priority: 2},
		{IP: "10.0.0.2", Port: 8080, Name: "backend-1", Priority: 2},
		{IP: "10.96.0.9", Port: 8080, Name: "backend-svc-0", Priority: 1},
	}, nil))
	if !strings.Contains(got, "server 10.0.0.1:8080;") || !strings.Contains(got, "server 10.0.0.2:8080;") {
		t.Errorf("主用 peer 不该带 backup:\n%s", got)
	}
	if !strings.Contains(got, "server 10.96.0.9:8080 backup;") {
		t.Errorf("低优先级 peer 应标 backup:\n%s", got)
	}
}

// 不写 modelType(老的 ModelRoute)必须走原来的 LLM 模板 —— 向后兼容。
func TestRenderRoute_EmptyModelTypeStaysLLM(t *testing.T) {
	got, err := RenderRoute(RouteData{Route: "kimi", Peers: []config.Peer{{IP: "10.0.0.1", Port: 8050}}})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "register_route") || !strings.Contains(got, "router_locations.inc") {
		t.Errorf("空 modelType 应仍渲染 LLM 引擎路由:\n%s", got)
	}
}

// video 路由必须走**标准 reload 通道**:和 LLM 路由一样,写进同一个 ConfigMap 的
// session_route_<route>.conf,由 reload sidecar 监听目录变化 → SIGHUP。
// 这里守住 sidecar 依赖的那条 listen 行格式:它靠正则从 conf 里提取 <route>.sock 判断
// 哪些 socket 还在用;格式对不上,这条路由的 socket 会被当成孤儿删掉 → dispatch 502。
func TestRenderRoute_VideoListenLineMatchesReloadSidecar(t *testing.T) {
	got, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	// 与 internal/reload 的 listenSockRe 同款:listen unix:<path>/<name>.sock;
	re := regexp.MustCompile(`(?m)^\s*listen\s+unix:(\S+\.sock)\s*;`)
	m := re.FindStringSubmatch(got)
	if m == nil {
		t.Fatalf("没有 reload sidecar 能识别的 listen unix:...sock; 行:\n%s", got)
	}
	if filepath.Base(m[1]) != "minimax-h3.sock" {
		t.Errorf("socket 名应为 <route>.sock,实际 %q", m[1])
	}
}

// route 名允许带连字符(如 minimax-h3),但 nginx **引用**变量时按 [A-Za-z0-9_] 取名 ——
// $fwdproto_minimax-h3 会被解析成 $fwdproto_minimax 再接字面量 "-h3",转发头拿到错值**且不报错**。
// 所以 map 生成的变量名必须净化;upstream / socket 名保持原样(它们不是变量,允许连字符)。
func TestRenderRoute_VideoSanitizesNginxVarNames(t *testing.T) {
	got, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	for _, want := range []string{
		"$fwdproto_minimax_h3",                                        // 变量名:连字符换成下划线
		"upstream backend_minimax-h3",                                 // upstream 名:原样
		"listen unix:/usr/local/openresty/nginx/sock/minimax-h3.sock", // socket 名:原样
	} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少 %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "$fwdproto_minimax-h3") || strings.Contains(got, "$fwdhost_minimax-h3") {
		t.Errorf("变量名里不该出现连字符:\n%s", got)
	}
}

// 只配一个 VIP peer(video 的推荐用法)时,不该出现 backup —— 单个 server 标 backup
// 会让 upstream 一个可用后端都没有,nginx 直接 502。
func TestRenderRoute_VideoSingleVipPeerIsNotBackup(t *testing.T) {
	got, _ := RenderRoute(videoData([]config.Peer{{IP: "10.108.240.122", Port: 8080, Name: "backend-svc-0", Priority: 1}}, nil))
	if !strings.Contains(got, "server 10.108.240.122:8080;") {
		t.Errorf("单 peer 应是主用(不带 backup):\n%s", got)
	}
}
