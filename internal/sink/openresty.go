package sink

import (
	"bytes"
	_ "embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"autoconfig/internal/config"
)

// luaVal 把 values 的字符串安全渲染成 lua 字面量:数字/布尔/nil 原样出,其余当字符串加引号 + 转义
// (防止值里含 , } 换行 " 破坏 conf 或注入)。
func luaVal(s string) string {
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	if s == "true" || s == "false" || s == "nil" {
		return s
	}
	s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", " ", "\r", " ").Replace(s)
	return `"` + s + `"`
}

// defaultRouteTmpl 内置路由模板(route.tmpl 编进二进制,controller 无需挂载模板文件)。
//
//go:embed route.tmpl
var defaultRouteTmpl string

// videoRouteTmpl:modelType=video 的路由模板(纯反向代理,不进 lua 引擎)。
//
//go:embed route_video.tmpl
var videoRouteTmpl string

// ModelType 取值(= CRD spec.modelType)。空 = llm,老的 ModelRoute 不写这个字段,行为不变。
const (
	ModelTypeLLM   = "llm"
	ModelTypeVideo = "video"
)

// video 模板的调优项与默认值。视频生成的请求体可能是 64MB 的 base64 图,
// 一条片子要 1~3 分钟、下载几十 MB,所以超时按小时给。
const (
	defaultVideoMaxBodySize    = "64m"
	defaultVideoProxyTimeout   = "3600s"
	defaultVideoConnectTimeout = "10s"
	// 单连接下载限速。视频是几十 MB 的大文件,不限速时几个客户端就能把节点网卡吃满,
	// 波及同一台机上的别的服务(openresty 是共享入口)。200Mbps = 25MB/s。
	defaultVideoRateLimit = "200Mbps"
	// 限速的豁免额度:前 N 字节全速。建任务/查询/删除都是几百字节的 JSON,
	// 不该被下载限速拖慢 —— 只有真正的大响应才会触发限速。
	defaultVideoRateLimitAfter = "1m"
	// 上传闸门的分组依据。默认按直连对端 IP —— 只在客户端直连时才是"每客户端";
	// 经网关时所有请求同一个 IP,那种部署要显式配成 $http_x_forwarded_for。
	defaultVideoUploadLimitKey = "$binary_remote_addr"
)

// videoPeer 是 video 模板看到的 peer:nginx upstream 的一行。
// Backup 对应 nginx 的 backup 标记 —— 只有主组全挂时才用(承载 backend-svc 这类静态兜底)。
type videoPeer struct {
	IP     string
	Port   int
	Name   string
	Backup bool
}

// nginxVarName 把 route 名净化成能安全用作 **nginx 变量名** 的形式:非 [A-Za-z0-9_] 一律换成 _。
//
// route 允许 [a-z0-9._-](如 modelforge-001-sglang-nvidia-a100)。nginx 在**声明**变量时不校验字符集
// (线上那条 LLM 路由的 set_by_lua_block $__<route>_init 带连字符也能起),但**引用**时脚本编译器
// 按 [A-Za-z0-9_] 取名字 —— $fwdproto_minimax-h3 会被解析成变量 $fwdproto_minimax 再接字面量 "-h3",
// 于是转发头拿到的值是错的,而且不报错。所以变量名必须净化。
func nginxVarName(route string) string {
	var b strings.Builder
	for _, r := range route {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// parseRate 把限速值翻成 nginx limit_rate 认的「字节/秒」。
//
// 接受两种写法:
//   - "200Mbps" / "1.5Gbps"(比特/秒,运维口径)→ 换算成字节/秒;
//   - nginx 原生写法("25m"、"512k"、纯数字)→ 原样透传。
//
// 换算按 1 Mbps = 1000000 bit/s(网络口径,不是 1024),再除以 8。
func parseRate(v string) (string, error) {
	s := strings.TrimSpace(v)
	if s == "" {
		return "", nil
	}
	low := strings.ToLower(s)
	mult := 0.0
	switch {
	case strings.HasSuffix(low, "gbps"):
		mult, low = 1e9, strings.TrimSuffix(low, "gbps")
	case strings.HasSuffix(low, "mbps"):
		mult, low = 1e6, strings.TrimSuffix(low, "mbps")
	case strings.HasSuffix(low, "kbps"):
		mult, low = 1e3, strings.TrimSuffix(low, "kbps")
	default:
		// nginx 原生写法,交给 nginx 自己校验(写错了 openresty -t 会报,不至于静默)。
		return s, nil
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(low), 64)
	if err != nil || n <= 0 {
		return "", fmt.Errorf("限速值 %q 解析失败:期望形如 200Mbps / 1.5Gbps,或 nginx 原生写法 25m", v)
	}
	return strconv.FormatInt(int64(n*mult/8), 10), nil
}

// checkVideoTiers:nginx upstream 只有「主用 / backup」两档,表达不了三层降级。
// 配了三档时不能静默压扁(中间那层会和最低层混为一谈,主用挂掉后流量可能直接落到 VIP),
// 直接报错让人改配置。
func checkVideoTiers(peers []config.Peer) error {
	seen := map[int]bool{}
	for _, p := range peers {
		seen[p.Priority] = true
	}
	if len(seen) <= 2 {
		return nil
	}
	tiers := make([]int, 0, len(seen))
	for p := range seen {
		tiers = append(tiers, p)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(tiers)))
	return fmt.Errorf("modelType: video 的 peers 最多两档优先级(nginx upstream 只有主用/backup),当前有 %d 档:%v;"+
		"请合并中间层,或直接只用一个 backend-svc(推荐)", len(tiers), tiers)
}

// videoRouteData 喂给 route_video.tmpl。
type videoRouteData struct {
	Route string
	// Var:Route 的变量名安全形式(见 nginxVarName)。只用于 map 生成的变量,
	// upstream / socket / 注释仍用原名。
	Var            string
	Peers          []videoPeer
	MaxBodySize    string
	ProxyTimeout   string
	ConnectTimeout string
	RateLimit      string
	RateLimitAfter string
	// 上传方向的闸门。nginx 没有请求体的字节级限速,只能靠"并发数 + 请求速率 + 体积上限"三道:
	// UploadConnLimit = 每 IP 同时在传的连接数;UploadReqLimit = 每 IP 请求速率(nginx 原生写法
	// 如 10r/s、30r/m);UploadReqBurst = 允许的突发条数。都不配 = 不限。
	UploadConnLimit string
	UploadReqLimit  string
	UploadReqBurst  string
	// UploadLimitKey:上面两个 zone 按什么分组。默认 $binary_remote_addr(直连对端 IP);
	// 经外层网关进来时那是同一个 IP,得换成 $http_x_forwarded_for 才有"每客户端"的语义。
	UploadLimitKey string
}

// toVideoPeers 把发现结果翻成 upstream 行:优先级最高的一组是主用,更低的全部标 backup。
//
// LLM 引擎里 priority 是 lua 自己按序挑;纯 nginx upstream 没有"优先级"概念,只有
// 主用/backup 两档。所以这里把 max(priority) 之外的都降成 backup —— 语义上等价于
// "backend-svc(VIP 兜底)平时不接流量,pod-IP 那层全挂了才顶上"。
func toVideoPeers(peers []config.Peer) []videoPeer {
	if len(peers) == 0 {
		return nil
	}
	top := peers[0].Priority
	for _, p := range peers {
		if p.Priority > top {
			top = p.Priority
		}
	}
	out := make([]videoPeer, 0, len(peers))
	for _, p := range peers {
		out = append(out, videoPeer{IP: p.IP, Port: p.Port, Name: p.Name, Backup: p.Priority < top})
	}
	return out
}

// RouteData 喂给 route.tmpl。Route 是结构(= socket 名/dict 名/路径 key);Extra 是任意调优项
// (原样渲染进 lua 返回表,text/template range map 按 key 排序 → 确定性);Peers 是发现结果。
type RouteData struct {
	Route string
	// ModelType = CRD spec.modelType。空/llm 走 lua 路由引擎;video 走纯反向代理模板。
	ModelType string
	Extra     map[string]string
	// Raw 是**已经是 lua 字面量**的片段(如 ttft_metrics 的嵌套 table),模板里原样输出、
	// 不过 luaVal。Extra 走 luaVal 会把非数字值加引号 —— 对 table 而言那就成了字符串,
	// 引擎读到的类型不对。仅由 operator 自己生成,不承载用户自由文本。
	Raw   map[string]string
	Peers []config.Peer
}

func RenderRoute(d RouteData) (string, error) {
	if d.ModelType == ModelTypeVideo {
		return renderVideoRoute(d)
	}
	tmpl, err := template.New("route").Funcs(template.FuncMap{"luaVal": luaVal}).Parse(defaultRouteTmpl)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, d); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// renderVideoRoute 渲染 modelType=video 的路由:纯反向代理,不生成 lua 路由表。
// Extra 在这里不是 lua 调优项,而是模板旋钮(CRD 侧已用 CEL 限定了可用 key)。
func renderVideoRoute(d RouteData) (string, error) {
	if err := checkVideoTiers(d.Peers); err != nil {
		return "", err
	}
	// 不配 = 不限速(渲染时整段跳过),不给默认值 —— 限速是要显式选择的策略,
	// 悄悄给一个默认上限会让人查半天"为什么下载只有 25MB/s"。
	rate, err := parseRate(d.Extra["rate_limit"])
	if err != nil {
		return "", err
	}
	rateAfter := ""
	if rate != "" {
		rateAfter = firstNonEmpty(d.Extra["rate_limit_after"], defaultVideoRateLimitAfter)
	}
	v := videoRouteData{
		Route:           d.Route,
		Var:             nginxVarName(d.Route),
		Peers:           toVideoPeers(d.Peers),
		MaxBodySize:     firstNonEmpty(d.Extra["max_body_size"], defaultVideoMaxBodySize),
		ProxyTimeout:    firstNonEmpty(d.Extra["proxy_timeout"], defaultVideoProxyTimeout),
		ConnectTimeout:  firstNonEmpty(d.Extra["connect_timeout"], defaultVideoConnectTimeout),
		RateLimit:       rate,
		RateLimitAfter:  rateAfter,
		UploadLimitKey:  firstNonEmpty(d.Extra["upload_limit_key"], defaultVideoUploadLimitKey),
		UploadConnLimit: d.Extra["upload_conn_limit"],
		UploadReqLimit:  d.Extra["upload_req_limit"],
		UploadReqBurst:  d.Extra["upload_req_burst"],
	}
	tmpl, terr := template.New("route_video").Parse(videoRouteTmpl)
	if err := terr; err != nil {
		return "", fmt.Errorf("parse video template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, v); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// ResolveSources builds a route's ordered peer list. If sources is set, it concatenates each
// source's discovered peers (applying that source's priority/maxConcurrency override) in order —
// e.g. CART(priority 1) then backends(priority 0). Otherwise it falls back to the single target.
// Names are assigned per source ("<target>-N"), so peers stay uniquely named across sources.
func ResolveSources(peersByTarget map[string][]config.Peer, sources []config.RouteSource, singleTarget string) []config.Peer {
	if len(sources) == 0 {
		return nameUnnamed(peersByTarget[singleTarget], singleTarget)
	}
	var all []config.Peer
	for _, src := range sources {
		for _, p := range nameUnnamed(peersByTarget[src.Target], src.Target) {
			if src.Priority != 0 {
				p.Priority = src.Priority
			}
			if src.MaxConcurrency != 0 {
				p.MaxConcurrency = src.MaxConcurrency
			}
			if src.ProbePath != "" {
				p.ProbePath = src.ProbePath
			}
			all = append(all, p)
		}
	}
	return all
}
