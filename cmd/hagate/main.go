// hagate —— master-standby 的 leader 选举 + 标签门控 sidecar:只有 leader 给自己 pod 打 active 标签,
// Service selector 匹配它 → 只有 leader 进 Service。与 controller/reload 分离的小二进制/镜像。
// SIGTERM 时主动释放 Lease + 摘标签 → 计划内切换近乎无缝。
//
//	hagate --lease openresty-ha --app-tcp 127.0.0.1:18080 --label-key openresty-active
//
// namespace/identity 走 downward API 环境变量(POD_NAMESPACE/POD_NAME)。
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"autoconfig/internal/hagate"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[hagate] ")

	lease := flag.String("lease", envOr("HA_LEASE", ""), "Lease 名(一组主备共用一个)")
	httpAddr := flag.String("http", envOr("HA_HTTP", ":8081"), "healthz 监听地址")
	appTCP := flag.String("app-tcp", envOr("HA_APP_TCP", ""), "本地 app host:port(非空则 leader 还要求它可连才打标签)")
	labelKey := flag.String("label-key", envOr("HA_LABEL_KEY", "ha-active"), "active 标签 key(Service selector 匹配它)")
	labelVal := flag.String("label-val", envOr("HA_LABEL_VAL", "true"), "active 标签 value")
	leaseDur := flag.Duration("lease-duration", envDur("HA_LEASE_DURATION", 8*time.Second), "租约有效期(硬崩 failover 上界)")
	renewDl := flag.Duration("renew-deadline", envDur("HA_RENEW_DEADLINE", 5*time.Second), "leader 续租超时")
	retry := flag.Duration("retry-period", envDur("HA_RETRY_PERIOD", 1*time.Second), "候选重试间隔")
	flag.Parse()

	ns := envOr("POD_NAMESPACE", "")
	id := envOr("POD_NAME", "")
	if *lease == "" || ns == "" || id == "" {
		log.Fatalf("需要 --lease + POD_NAMESPACE + POD_NAME(downward API)")
	}
	err := hagate.Run(hagate.Config{
		Lease: *lease, Namespace: ns, Identity: id,
		HTTPAddr: *httpAddr, AppTCP: *appTCP,
		LabelKey: *labelKey, LabelVal: *labelVal,
		LeaseDuration: *leaseDur, RenewDeadline: *renewDl, RetryPeriod: *retry,
	})
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
