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

// 不配 rate_limit = 完全不限速:一行 limit_rate 都不该渲染。
// 悄悄给一个默认上限会让人查半天"为什么下载只有 25MB/s"。
func TestRenderRoute_VideoNoRateLimitByDefault(t *testing.T) {
	got, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(got, "limit_rate") {
		t.Errorf("没配 rate_limit 就不该出现 limit_rate:\n%s", got)
	}
}

// 配了才限速;换算成 nginx limit_rate 认的字节/秒(200e6/8 = 25,000,000),
// 并带上伴随默认的 limit_rate_after(让小 JSON 响应全速)。
func TestRenderRoute_VideoRateLimitWhenConfigured(t *testing.T) {
	got, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"rate_limit": "200Mbps"}))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{"limit_rate       25000000;", "limit_rate_after 1m;"} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少 %q:\n%s", want, got)
		}
	}
}

func TestRenderRoute_VideoRateLimitOverride(t *testing.T) {
	cases := []struct{ in, want string }{
		{"1Gbps", "limit_rate       125000000;"}, // 1e9/8
		{"400mbps", "limit_rate       50000000;"},
		{"25m", "limit_rate       25m;"}, // nginx 原生写法原样透传
		{"512k", "limit_rate       512k;"},
	}
	for _, c := range cases {
		got, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
			map[string]string{"rate_limit": c.in, "rate_limit_after": "4m"}))
		if err != nil {
			t.Fatalf("rate_limit=%q: %v", c.in, err)
		}
		if !strings.Contains(got, c.want) {
			t.Errorf("rate_limit=%q 期望 %q,实际:\n%s", c.in, c.want, got)
		}
		if !strings.Contains(got, "limit_rate_after 4m;") {
			t.Errorf("rate_limit_after 覆盖未生效")
		}
	}
}

func TestRenderRoute_VideoRateLimitRejectsGarbage(t *testing.T) {
	_, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"rate_limit": "很快Mbps"}))
	if err == nil {
		t.Fatal("非法限速值应报错,而不是渲染出坏 conf")
	}
}

// nginx upstream 只有主用/backup 两档,表达不了三层降级。
// 静默压扁会让中间层(pod IP)和最低层(VIP)混为一谈 —— 主用挂掉后流量可能直接落到 VIP,
// 所以必须报错让人改配置。
func TestRenderRoute_VideoRejectsThreeTiers(t *testing.T) {
	_, err := RenderRoute(videoData([]config.Peer{
		{IP: "10.0.0.1", Port: 8080, Name: "cart-0", Priority: 3},
		{IP: "10.0.0.2", Port: 8080, Name: "backend-0", Priority: 2},
		{IP: "10.96.0.9", Port: 8080, Name: "backend-svc-0", Priority: 1},
	}, nil))
	if err == nil {
		t.Fatal("三档优先级应被拒绝(nginx 只有主用/backup 两档)")
	}
	if !strings.Contains(err.Error(), "最多两档优先级") {
		t.Errorf("错误信息应说清原因,实际:%v", err)
	}
}

// 两档仍然正常(主用 + backup),这是 video 允许的最复杂形态。
func TestRenderRoute_VideoAllowsTwoTiers(t *testing.T) {
	got, err := RenderRoute(videoData([]config.Peer{
		{IP: "10.0.0.1", Port: 8080, Name: "backend-0", Priority: 2},
		{IP: "10.96.0.9", Port: 8080, Name: "backend-svc-0", Priority: 1},
	}, nil))
	if err != nil {
		t.Fatalf("两档不该报错:%v", err)
	}
	if !strings.Contains(got, "server 10.96.0.9:8080 backup;") {
		t.Errorf("低优先级应标 backup:\n%s", got)
	}
}

// limit_rate 在 proxy_buffering off 下**完全不生效**(实测 50MB:静态 4.99s /
// buffering on 4.61s / buffering off 0.089s;proxy_limit_rate 同样只在开缓冲时有效)。
// 所以配了限速就必须开缓冲,并用 proxy_max_temp_file_size 0 避免落盘。
// 这两件事必须成对出现,拆开任意一半都会让限速变成死配置。
func TestRenderRoute_VideoRateLimitRequiresBuffering(t *testing.T) {
	limited, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"rate_limit": "200Mbps"}))
	if !strings.Contains(limited, "proxy_buffering on;") {
		t.Errorf("配了限速必须开缓冲,否则 limit_rate 被忽略:\n%s", limited)
	}
	if !strings.Contains(limited, "proxy_max_temp_file_size 0;") {
		t.Errorf("开缓冲时要禁临时文件,免得几十 MB 的视频落盘:\n%s", limited)
	}
	if strings.Contains(limited, "proxy_buffering off;") {
		t.Errorf("同一个 location 里不该又出现 proxy_buffering off:\n%s", limited)
	}

	plain, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	if !strings.Contains(plain, "proxy_buffering off;") {
		t.Errorf("没配限速时应保持不缓冲(边收边发):\n%s", plain)
	}
	if strings.Contains(plain, "proxy_max_temp_file_size") {
		t.Errorf("没开缓冲就不需要 proxy_max_temp_file_size:\n%s", plain)
	}
}

// 上传方向:不配 = 一行闸门都不渲染;配了则 zone 声明(http 级,server 块之外)与
// 引用(server 内)必须成对出现 —— 只有其中一半 nginx 直接起不来。
func TestRenderRoute_VideoUploadLimits(t *testing.T) {
	plain, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	for _, s := range []string{"limit_conn", "limit_req"} {
		if strings.Contains(plain, s) {
			t.Errorf("没配上传闸门时不该出现 %s:\n%s", s, plain)
		}
	}

	out, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, map[string]string{
		"upload_conn_limit": "4", "upload_req_limit": "10r/s", "upload_req_burst": "20",
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"limit_conn_zone $http_x_real_ip zone=upconn_minimax_h3:10m;",
		"limit_conn upconn_minimax_h3 4;",
		"limit_req_zone $http_x_real_ip zone=upreq_minimax_h3:10m rate=10r/s;",
		"limit_req  zone=upreq_minimax_h3 burst=20;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("缺 %q:\n%s", want, out)
		}
	}
	// zone 是 http 级指令,必须落在 server 块之前,否则 nginx 报 "not allowed here"
	if strings.Index(out, "limit_conn_zone") > strings.Index(out, "server {") {
		t.Errorf("limit_conn_zone 必须在 server 块之外:\n%s", out)
	}
}

// upload_limit_key 决定两个 zone 按什么分组。默认 $binary_remote_addr 是**直连对端**的 IP;
// 经外层网关(如 phanrouter)进来时所有请求同一个 IP,"每 IP 限制"就塌成"全局限制",
// 那种部署必须能换成 $http_x_forwarded_for —— 这个用例守住这个可配性。
func TestRenderRoute_VideoUploadLimitKey(t *testing.T) {
	def, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"upload_conn_limit": "4", "upload_req_limit": "10r/s"}))
	if !strings.Contains(def, "limit_conn_zone $http_x_real_ip zone=upconn_minimax_h3") ||
		!strings.Contains(def, "limit_req_zone $http_x_real_ip zone=upreq_minimax_h3") {
		t.Errorf("默认 key 应是 $http_x_real_ip:\n%s", def)
	}
	// $binary_remote_addr 在 unix socket 后面恒为 "unix:",所有请求落同一个桶 ——
	// 绝不能出现在 zone 指令里(实测见 tools/minimax-h3/tests/t25)。
	for _, line := range strings.Split(def, "\n") {
		d := strings.TrimSpace(line)
		if (strings.HasPrefix(d, "limit_conn_zone") || strings.HasPrefix(d, "limit_req_zone")) &&
			strings.Contains(d, "$binary_remote_addr") {
			t.Errorf("zone 不该用 $binary_remote_addr(unix socket 后恒为 unix:): %s", d)
		}
	}

	xff, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, map[string]string{
		"upload_conn_limit": "4", "upload_req_limit": "10r/s",
		"upload_limit_key": "$http_x_forwarded_for",
	}))
	if !strings.Contains(xff, "limit_conn_zone $http_x_forwarded_for zone=upconn_minimax_h3") ||
		!strings.Contains(xff, "limit_req_zone $http_x_forwarded_for zone=upreq_minimax_h3") {
		t.Errorf("配了 upload_limit_key 后两个 zone 都要跟着换:\n%s", xff)
	}
	// 只看**指令行**:模板的注释里本来就提到 $binary_remote_addr(解释默认值是什么),
	// 直接 Contains 整份 conf 会把注释也算进去,断言恒假。
	for _, line := range strings.Split(xff, "\n") {
		d := strings.TrimSpace(line)
		if strings.HasPrefix(d, "limit_conn_zone") || strings.HasPrefix(d, "limit_req_zone") {
			if strings.Contains(d, "$http_x_real_ip") {
				t.Errorf("zone 指令还留着默认 key: %s", d)
			}
		}
	}
}

// 鉴权默认开启,key 读 openresty 镜像里的 lua/api_keys.lua(与 LLM 同一张表)——
// **key 不出现在渲染结果里**,这正是这版改动的目的。下载路径默认放行。
func TestRenderRoute_VideoAuth(t *testing.T) {
	out, err := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`access_by_lua_block {`,
		`require("api_keys").check(`,
		`^/v2/video_generation/[^/]+/content$`,
		`ngx.status = 401`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("缺 %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "sk-") {
		t.Errorf("渲染结果里不该出现任何 key:\n%s", out)
	}

	// 显式关闭
	off, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"auth": "false"}))
	if strings.Contains(off, "access_by_lua_block") {
		t.Errorf("auth=false 时不该渲染鉴权:\n%s", off)
	}

	// 放行路径可关掉(连下载也要 key)
	strict, _ := RenderRoute(videoData([]config.Peer{{IP: "10.0.0.1", Port: 8080}},
		map[string]string{"auth_public_paths": ""}))
	if strings.Contains(strict, "/content$") {
		t.Errorf("auth_public_paths 置空后不该再放行下载:\n%s", strict)
	}
	if !strings.Contains(strict, `require("api_keys").check(`) {
		t.Errorf("放行路径关掉后鉴权本身仍应在:\n%s", strict)
	}
}
