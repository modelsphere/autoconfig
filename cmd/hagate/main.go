// hagate —— master-standby 的 leader 选举门控 sidecar:2 副本里只有 leader 让 /healthz=200
// (readinessProbe 打它)→ 只有 leader 进 Service。与 controller/reload 分离的小二进制/镜像。
//
//	hagate --lease openresty-ha --http :8081 --app-tcp 127.0.0.1:18080
//
// namespace/identity 走 downward API 环境变量(POD_NAMESPACE/POD_NAME)。
package main

import (
	"flag"
	"log"
	"os"

	"autoconfig/internal/hagate"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[hagate] ")

	lease := flag.String("lease", envOr("HA_LEASE", ""), "Lease 名(一组主备共用一个)")
	httpAddr := flag.String("http", envOr("HA_HTTP", ":8081"), "healthz 监听地址")
	appTCP := flag.String("app-tcp", envOr("HA_APP_TCP", ""), "本地 app host:port(非空则 leader 还要求它可连)")
	flag.Parse()

	ns := envOr("POD_NAMESPACE", "")
	id := envOr("POD_NAME", "")
	if *lease == "" || ns == "" || id == "" {
		log.Fatalf("需要 --lease + POD_NAMESPACE + POD_NAME(downward API)")
	}
	if err := hagate.Run(*lease, ns, id, *httpAddr, *appTCP); err != nil {
		log.Fatalf("%v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
