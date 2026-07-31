// Package reload implements the reload-sidecar: watch a ConfigMap-mounted file/dir and SIGHUP the app.
package reload

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// listenSockRe 抓 conf 里 `listen unix:/path/<name>.sock;` 的 socket 路径。
var listenSockRe = regexp.MustCompile(`listen\s+unix:(\S+\.sock)`)

// cleanupOrphanSockets 扫 confDir 全部 *.conf 收集当前在用的 <name>.sock,删 sockDir 里不在其中的 .sock。
// 模型删除后其 conf 从 ConfigMap 消失,但 nginx reload 不会 unlink 老 worker 已建的 socket 文件 → 残留。
// 只删「无任何 conf 引用」的,故绝不会误删在用 socket。
func cleanupOrphanSockets(sockDir, confDir string) {
	active := map[string]bool{}
	confs, _ := filepath.Glob(filepath.Join(confDir, "*.conf"))
	for _, c := range confs {
		b, err := os.ReadFile(c)
		if err != nil {
			return // 读不全宁可不删(避免误判成孤儿)
		}
		for _, m := range listenSockRe.FindAllStringSubmatch(string(b), -1) {
			active[filepath.Base(m[1])] = true
		}
	}
	socks, _ := filepath.Glob(filepath.Join(sockDir, "*.sock"))
	for _, s := range socks {
		if !active[filepath.Base(s)] {
			if err := os.Remove(s); err == nil {
				log.Printf("[reload] removed orphan socket %s (no conf references it)", s)
			}
		}
	}
}

// Run watches watchPath (its dir, since ConfigMap updates swap the ..data symlink) and, on change,
// SIGHUPs the process whose argv[0] matches procMatch (see findPID). Blocks. Needs shareProcessNamespace.
// sockDir(可选,路径路由用):reload 前删掉「无对应 conf 的孤儿 <name>.sock」——模型删除后 nginx
// reload 不会 unlink 残留 unix socket 文件,dispatch 打它会 502 且文件长期堆积。为空则不做清理。
func Run(watchPath, procMatch, sockDir string) error {
	if watchPath == "" || procMatch == "" {
		return fmt.Errorf("reload mode needs --watch and --process")
	}
	dir := watchPath
	if fi, err := os.Stat(watchPath); err == nil && !fi.IsDir() {
		dir = filepath.Dir(watchPath)
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.Add(dir); err != nil {
		return fmt.Errorf("watch %s: %w", dir, err)
	}
	log.Printf("[reload] watching %s -> SIGHUP process matching %q", dir, procMatch)

	var timer *time.Timer
	trigger := func() {
		if sockDir != "" {
			cleanupOrphanSockets(sockDir, dir) // reload 前清孤儿 socket(见 sockDir 注释)
		}
		pid := findPID(procMatch)
		if pid <= 0 {
			log.Printf("[reload] process %q not found (skip SIGHUP)", procMatch)
			return
		}
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			log.Printf("[reload] SIGHUP pid %d failed: %v", pid, err)
			return
		}
		log.Printf("[reload] config changed -> SIGHUP pid %d (%s)", pid, procMatch)
	}
	for {
		select {
		case <-w.Events:
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(2*time.Second, trigger) // debounce the burst of CM-update events
		case err := <-w.Errors:
			log.Printf("[reload] watch error: %v", err)
		}
	}
}

// findPID scans /proc for a process whose **argv[0]** matches match —— 只比进程标识(argv[0]),
// 不比整条 cmdline。这样能稳当排除「参数里含 match」的进程(如 reload/hagate sidecar 自己的
// `--process nginx: master` 参数)。argv[0] 用 cmdline 首段(comm 会截断 15 字符)。
// 匹配规则(覆盖 nginx 改写 argv[0] + 普通二进制两种):argv[0] == match、basename(argv[0]) == match、
// 或 argv[0] 以 match 开头(nginx master 的 argv[0] = "nginx: master process ...")。
func findPID(match string) int {
	procs, _ := filepath.Glob("/proc/[0-9]*")
	self := os.Getpid()
	for _, d := range procs {
		b, err := os.ReadFile(filepath.Join(d, "cmdline"))
		if err != nil {
			continue
		}
		argv0, _, _ := strings.Cut(string(b), "\x00") // cmdline 首段 = argv[0]
		if argv0 == "" {
			continue
		}
		if argv0 == match || filepath.Base(argv0) == match || strings.HasPrefix(argv0, match) {
			var pid int
			if _, err := fmt.Sscanf(filepath.Base(d), "%d", &pid); err == nil && pid != self {
				return pid
			}
		}
	}
	return 0
}
