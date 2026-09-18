// Package reload implements the reload-sidecar: watch a ConfigMap-mounted file/dir and SIGHUP the app.
package reload

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// listenSockRe 抓 conf 里 `listen unix:/path/<name>.sock;` 的 socket 路径。
var listenSockRe = regexp.MustCompile(`listen\s+unix:(\S+\.sock)`)

// soAcceptCon: /proc/net/unix Flags 列里 listening socket 的标志位(SO_ACCEPTCON)。
const soAcceptCon = 0x10000

// hasListener 判断某 unix socket 路径当前是否有进程 bind+listen(读内核 /proc/net/unix,只读无副作用)。
// 行格式: Num RefCount Protocol Flags Type St Inode [Path];listening = Flags&0x10000!=0 且 Path 匹配。
// 读不到 /proc/net/unix(非 linux / 权限)→ 保守返回 true(视为在用,不删),宁可漏删不误删。
// 声明为 var 便于单测注入(cleanupOrphanSockets 的 reap 逻辑可脱离真 socket / linux 测)。
var hasListener = func(sockPath string) bool {
	f, err := os.Open("/proc/net/unix")
	if err != nil {
		return true
	}
	defer f.Close()
	target := filepath.Clean(sockPath)
	sc := bufio.NewScanner(f)
	sc.Scan() // 跳过表头
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 8 {
			continue // 无 Path 列 = 未命名 socket,不是我们要找的
		}
		flags, err := strconv.ParseUint(fields[3], 16, 32)
		if err != nil || flags&soAcceptCon == 0 {
			continue // 非 listening
		}
		if filepath.Clean(fields[7]) == target {
			return true
		}
	}
	return false
}

// cleanupOrphanSockets 删 sockDir 里既「无 conf 引用」又「当前无进程 listen」的死 socket 文件。
// 模型删除后其 conf 从 ConfigMap 消失,但 nginx reload 不会 unlink 老 worker 已建的 socket 文件 → 残留。
// 两个条件都要:
//   - 无 conf 引用:排除「正常模型的 socket」(它被 conf listen)。
//   - 无 listener:排除「手动创建/外部进程正在用」及「刚删模型但老 worker 还在 drain listen」的 socket
//     → 绝不误删任何在用 socket。刚删模型的 socket 本轮因老 worker 还 listen 而暂留,老 worker 退出后
//     下一轮 cleanup(下次 conf 变化)再 reap;此时 conf key 已删、dispatch 不再路由到它,无 502 风险。
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
		if active[filepath.Base(s)] {
			continue // conf 还引用 → 在用,保留
		}
		if hasListener(s) {
			continue // 无 conf 引用但仍有进程 listen(手动 socket / 老 worker 未退)→ 保留,下轮再 reap
		}
		if err := os.Remove(s); err == nil {
			log.Printf("[reload] removed orphan socket %s (no conf ref & no listener)", s)
		}
	}
}

// watchDirs maps each requested path to the directory to hand fsnotify. A ConfigMap or Secret
// update swaps the ..data symlink rather than rewriting the file in place, so watching a file
// directly would miss every update -- we always watch its parent directory.
func watchDirs(paths []string) []string {
	dirs := make([]string, 0, len(paths))
	seen := map[string]bool{}
	for _, p := range paths {
		d := p
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			d = filepath.Dir(p)
		}
		d = filepath.Clean(d)
		if seen[d] {
			continue // same directory named twice (or a file plus its own dir) -- add it once
		}
		seen[d] = true
		dirs = append(dirs, d)
	}
	return dirs
}

// Run watches every path in watchPaths (each one's directory, since ConfigMap/Secret updates swap
// the ..data symlink) and, on any change, SIGHUPs the process whose argv[0] matches procMatch (see
// findPID). Blocks. Needs shareProcessNamespace.
//
// Several paths are supported because a single reload can be driven by more than one mounted
// volume -- openresty takes its route confs from a ConfigMap and its API keys from a Secret, and
// a change to either has to reach the same nginx master. A reload is idempotent, so one debounce
// timer shared by all paths is exactly right: a burst touching both mounts SIGHUPs once.
//
// sockDir(可选,路径路由用):reload 前删掉「无对应 conf 的孤儿 <name>.sock」——模型删除后 nginx
// reload 不会 unlink 残留 unix socket 文件,dispatch 打它会 502 且文件长期堆积。为空则不做清理。
// The orphan scan stays bound to watchPaths[0] by convention: it looks for `listen unix:` lines in
// *.conf, which only the route-conf directory has. Passing the first path keeps that contract
// explicit -- callers must list the route-conf mount first.
func Run(watchPaths []string, procMatch, sockDir string) error {
	if len(watchPaths) == 0 || watchPaths[0] == "" || procMatch == "" {
		return fmt.Errorf("reload mode needs --watch and --process")
	}
	dirs := watchDirs(watchPaths)
	confDir := dirs[0] // see the sockDir note above: orphan scan reads the route-conf dir only
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	for _, d := range dirs {
		if err := w.Add(d); err != nil {
			return fmt.Errorf("watch %s: %w", d, err)
		}
	}
	log.Printf("[reload] watching %s -> SIGHUP process matching %q", strings.Join(dirs, ", "), procMatch)

	var timer *time.Timer
	trigger := func() {
		if sockDir != "" {
			cleanupOrphanSockets(sockDir, confDir) // reload 前清孤儿 socket(见 sockDir 注释)
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
