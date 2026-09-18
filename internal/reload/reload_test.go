package reload

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// cleanupOrphanSockets 的 reap 决策:只删「无 conf 引用 且 无 listener」,在用的一律保留。
// 注入 hasListener 桩,脱离真 socket / linux。
func TestCleanupOrphanSockets_ReapDecision(t *testing.T) {
	sockDir := t.TempDir()
	confDir := t.TempDir()

	// conf 引用 glm.sock(正常在用模型)
	if err := os.WriteFile(filepath.Join(confDir, "glm.conf"),
		[]byte("server { listen unix:/any/dir/glm.sock; }"), 0o644); err != nil {
		t.Fatal(err)
	}
	mk := func(n string) {
		if err := os.WriteFile(filepath.Join(sockDir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("glm.sock")    // conf 引用 → 保留
	mk("orphan.sock") // 无 conf、无 listener → 删
	mk("inuse.sock")  // 无 conf 但有 listener(如手动创建)→ 保留

	orig := hasListener
	defer func() { hasListener = orig }()
	hasListener = func(p string) bool { return filepath.Base(p) == "inuse.sock" }

	cleanupOrphanSockets(sockDir, confDir)

	exists := func(n string) bool { _, err := os.Stat(filepath.Join(sockDir, n)); return err == nil }
	if !exists("glm.sock") {
		t.Error("conf 引用的 socket 被误删")
	}
	if exists("orphan.sock") {
		t.Error("孤儿(无 conf 无 listener)未被删")
	}
	if !exists("inuse.sock") {
		t.Error("在用(有 listener)的 socket 被误删")
	}
}

// conf 读不全时保守不删(避免把在用的误判成孤儿)。
func TestCleanupOrphanSockets_UnreadableConfSkips(t *testing.T) {
	sockDir := t.TempDir()
	confDir := t.TempDir()
	// 一个不可读的 conf → ReadFile 失败 → 整个 cleanup 提前 return,不删任何东西
	bad := filepath.Join(confDir, "bad.conf")
	if err := os.WriteFile(bad, []byte("x"), 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(bad, 0o644)
	if os.Geteuid() == 0 {
		t.Skip("root 能读 0000 文件,跳过")
	}
	orphan := filepath.Join(sockDir, "orphan.sock")
	os.WriteFile(orphan, nil, 0o644)
	orig := hasListener
	defer func() { hasListener = orig }()
	hasListener = func(string) bool { return false }

	cleanupOrphanSockets(sockDir, confDir)
	if _, err := os.Stat(orphan); err != nil {
		t.Error("conf 读不全时不应删任何 socket")
	}
}

// hasListener 真实读 /proc/net/unix(仅 linux):真 listener → true,普通文件 → false。
func TestHasListener_Real(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hasListener 读 /proc/net/unix,仅 linux")
	}
	dir := t.TempDir()
	live := filepath.Join(dir, "live.sock")
	l, err := net.Listen("unix", live)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if !hasListener(live) {
		t.Error("真 listener 应返回 true")
	}
	dead := filepath.Join(dir, "dead.sock")
	os.WriteFile(dead, nil, 0o644)
	if hasListener(dead) {
		t.Error("无 listener 的普通文件应返回 false")
	}
}

// watchDirs: a file resolves to its parent dir (ConfigMap/Secret updates swap the ..data symlink,
// so watching the file itself would miss them), directories pass through, and duplicates collapse.
// Order must be preserved -- Run treats the first path as the route-conf dir for the orphan scan.
func TestWatchDirs_FileToParentAndDedup(t *testing.T) {
	routes := t.TempDir()
	keys := t.TempDir()
	file := filepath.Join(keys, "api_keys.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := watchDirs([]string{routes, file, keys, routes})
	want := []string{routes, keys}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: expected %s, got %s", i, want[i], got[i])
		}
	}
}

// Run's argument validation: no paths, an empty first path, or no --process is a usage error.
// (The happy path blocks forever, so it is not unit-testable here.)
func TestRun_RejectsMissingArgs(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		proc  string
	}{
		{"no paths", nil, "nginx: master"},
		{"empty first path", []string{""}, "nginx: master"},
		{"no process", []string{t.TempDir()}, ""},
	}
	for _, c := range cases {
		if err := Run(c.paths, c.proc, ""); err == nil {
			t.Errorf("%s: expected an error, got nil", c.name)
		}
	}
}
