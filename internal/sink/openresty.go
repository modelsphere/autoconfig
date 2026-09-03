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
	// download_conn_limit 的默认值。配了 rate_limit 但没显式给并发上限时,
	// 不能退化成"每连接各占满 rate_limit"——那样总下载带宽会随并发数无限放大,
	// rate_limit 这个总量上限形同虚设。给个安全默认值(10 并发),配合 rate_limit 均分,
	// 保住"全局下载带宽 ≤ rate_limit"这个承诺;要更大的并发数,显式配 download_conn_limit。
	defaultVideoDownloadConnLimit = "10"
	// 上传闸门的分组依据。
	//
	// **不能用 $binary_remote_addr**:路由是挂在 unix socket 上的(dispatch 按 /<route>/
	// 分发过来),那一跳没有 IP —— 实测路由层的 $remote_addr 恒等于字符串 "unix:",
	// 于是每个请求算出同一个 key,zone 里所有请求落进同一个桶,
	// "每 IP 限 4 条"直接塌成"整个服务限 4 条"(与外层是不是网关无关,是结构决定的)。
	//
	// 改用 $http_x_real_ip:dispatch 转发时设了 X-Real-IP = 它自己的 $remote_addr,
	// 也就是 openresty 的直连对端 —— 客户端直连时就是客户端本身,经 phanrouter 时是
	// phanrouter 的 IP。要精确到"每真实客户端",配 upload_limit_key 取 XFF 最左一跳
	// (前提是外层确实透传 X-Forwarded-For)。
	defaultVideoUploadLimitKey = "$http_x_real_ip"
	// 免鉴权路径:下载。任务 id 是 UUID,等于一次性能力 URL。
	defaultVideoAuthPublicPaths = `^/v2/video_generation/[^/]+/content$`
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
	// 上传方向的闸门。nginx 没有请求体的字节级限速,只能靠"并发数 + 请求速率 + 体积上限"三道:
	// UploadConnLimit = 每 IP 同时在传的连接数;UploadReqLimit = 每 IP 请求速率(nginx 原生写法
	// 如 10r/s、30r/m);UploadReqBurst = 允许的突发条数。都不配 = 不限。
	UploadConnLimit string
	UploadReqLimit  string
	UploadReqBurst  string
	// UploadLimitKey:上面两个 zone 按什么分组。默认 $binary_remote_addr(直连对端 IP);
	// 经外层网关进来时那是同一个 IP,得换成 $http_x_forwarded_for 才有"每客户端"的语义。
	UploadLimitKey string
	// Auth:是否开启 Bearer 鉴权。key 不在这里配 —— 读 openresty 镜像里的
	// lua/api_keys.lua,与 LLM 路由同一张表(密钥不进 CR / values / git)。
	Auth bool
	// AuthPublicPaths:免鉴权路径的正则(ngx.re 语法,不带 ~ 前缀)。
	// 默认放行下载 —— content.url 交给最终用户,浏览器不会带 Authorization 头。
	AuthPublicPaths string
	// DownloadConnLimit:**全局(所有客户端合计)** 下载并发连接上限。配了则:
	//   ① 下载路径(/content)拆一个独立 location,挂 limit_conn —— key 用【常量】(每 route 一个
	//      固定值),所以是【单一桶、全局计数】,不是每客户端;超限返 429。
	//   ② 每连接 limit_rate = 总带宽 rate_limit / DownloadConnLimit(见 DownloadRate) → 全局下载
	//      带宽 ≤ rate_limit,而非每连接各占满 rate_limit。
	// 配了 rate_limit 但没显式给这项 = 落默认值(defaultVideoDownloadConnLimit,当前 10),
	// 不会退化成不限并发——那样总带宽随并发数无限放大,rate_limit 就没意义了。
	// 完全没配 rate_limit(不限速)时也不会有这个字段:没选限速策略,没有总量上限要保护。
	DownloadConnLimit string
	// DownloadRate:下载 location 的每连接 limit_rate(字节/秒)= 总带宽 ÷ DownloadConnLimit。
	DownloadRate string
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
	// download_conn_limit:全局下载并发上限。配了就把 rate_limit(总带宽)按并发数均分到每连接,
	// 并给下载路径挂全局 limit_conn。
	// 配了 rate_limit 但没显式给这项时,**不能**退化成"每连接各占满 rate_limit"——总带宽会随
	// 并发数无限放大,rate_limit 这个总量上限就是摆设,所以落一个安全默认值(见常量注释)。
	// 完全没配 rate_limit 时不下这个默认值:没选限速策略,谈不上要保护什么总量上限。
	downloadConnLimit := strings.TrimSpace(d.Extra["download_conn_limit"])
	if downloadConnLimit == "" && rate != "" {
		downloadConnLimit = defaultVideoDownloadConnLimit
	}
	downloadRate := ""
	if downloadConnLimit != "" {
		n, cerr := strconv.Atoi(downloadConnLimit)
		if cerr != nil || n <= 0 {
			return "", fmt.Errorf("download_conn_limit %q 无效:需为正整数", downloadConnLimit)
		}
		if rate == "" {
			return "", fmt.Errorf("download_conn_limit 需要同时配 rate_limit(总下载带宽,均分到每连接)")
		}
		total, perr := strconv.ParseInt(rate, 10, 64)
		if perr != nil {
			// rate 是 nginx 原生写法(如 25m)时无法按连接均分 —— 让运维改成 Mbps/Gbps 或纯字节。
			return "", fmt.Errorf("download_conn_limit 需要 rate_limit 为 Mbps/Gbps 或纯字节数,当前 %q 无法按连接均分", d.Extra["rate_limit"])
		}
		per := total / int64(n)
		if per < 1 {
			per = 1 // 均分到 0 会让 nginx 报错;钳到 1 字节/秒(病态配置,但不至于起不来)
		}
		downloadRate = strconv.FormatInt(per, 10)
	}
	v := videoRouteData{
		Route:             d.Route,
		Var:               nginxVarName(d.Route),
		Peers:             toVideoPeers(d.Peers),
		MaxBodySize:       firstNonEmpty(d.Extra["max_body_size"], defaultVideoMaxBodySize),
		ProxyTimeout:      firstNonEmpty(d.Extra["proxy_timeout"], defaultVideoProxyTimeout),
		ConnectTimeout:    firstNonEmpty(d.Extra["connect_timeout"], defaultVideoConnectTimeout),
		UploadLimitKey:    firstNonEmpty(d.Extra["upload_limit_key"], defaultVideoUploadLimitKey),
		Auth:              d.Extra["auth"] != "false",
		AuthPublicPaths:   authPublicPaths(d.Extra),
		UploadConnLimit:   d.Extra["upload_conn_limit"],
		UploadReqLimit:    d.Extra["upload_req_limit"],
		UploadReqBurst:    d.Extra["upload_req_burst"],
		DownloadConnLimit: downloadConnLimit,
		DownloadRate:      downloadRate,
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

// authPublicPaths 取免鉴权路径。不能用 firstNonEmpty —— 那样分不清"没配"(要默认值)
// 和"显式置空"(= 连下载也要带 key),后者会被悄悄回落成默认,鉴权范围与配置不符。
func authPublicPaths(extra map[string]string) string {
	if v, ok := extra["auth_public_paths"]; ok {
		return v
	}
	return defaultVideoAuthPublicPaths
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
