// reload —— 一个极简的 sidecar 程序:watch 挂载的 ConfigMap 文件/目录,
// 变化就给同 pod 的主进程(nginx master / cache-aware-router)发 SIGHUP 优雅重载。
// 与 autoconfig controller 分离,单独的小二进制/镜像;靠 shareProcessNamespace 发信号。
//
//	reload --watch /cfg --process cache-aware-router
//	reload --watch /watch --process "nginx: master"
//	reload --watch /watch --watch /keys --process "nginx: master"   # route confs + API-key Secret
package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"autoconfig/internal/reload"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[reload] ")

	var watch multiFlag
	flag.Var(&watch, "watch", "file/dir to watch; repeat to watch several mounts (first one must be the route-conf dir, see reload.Run)")
	proc := flag.String("process", envOr("PS_PROCESS", ""), "process cmdline substring to SIGHUP")
	sockDir := flag.String("sock-dir", envOr("PS_SOCK_DIR", ""), "可选:reload 前清此目录里无 conf 引用的孤儿 *.sock(路径路由)")
	flag.Parse()

	// PS_WATCH stays a single path: it predates multi-path support and every existing deployment
	// passes --watch anyway. An explicit --watch (even one) wins over the environment.
	if len(watch) == 0 {
		if v := os.Getenv("PS_WATCH"); v != "" {
			watch = append(watch, v)
		}
	}

	if err := reload.Run(watch, *proc, *sockDir); err != nil {
		log.Fatalf("%v", err)
	}
}

// multiFlag collects a repeatable string flag, so `--watch a --watch b` yields both paths.
// Order matters: reload.Run treats the first path as the route-conf dir for the orphan-socket scan.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
