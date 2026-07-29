// reload —— 一个极简的 sidecar 程序:watch 挂载的 ConfigMap 文件/目录,
// 变化就给同 pod 的主进程(nginx master / cache-aware-router)发 SIGHUP 优雅重载。
// 与 autoconfig controller 分离,单独的小二进制/镜像;靠 shareProcessNamespace 发信号。
//
//	reload --watch /cfg --process cache-aware-router
//	reload --watch /watch --process "nginx: master"
package main

import (
	"flag"
	"log"
	"os"

	"autoconfig/internal/reload"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[reload] ")

	watch := flag.String("watch", envOr("PS_WATCH", ""), "file/dir to watch")
	proc := flag.String("process", envOr("PS_PROCESS", ""), "process cmdline substring to SIGHUP")
	flag.Parse()

	if err := reload.Run(*watch, *proc); err != nil {
		log.Fatalf("%v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
