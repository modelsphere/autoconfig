// Package reload implements the reload-sidecar: watch a ConfigMap-mounted file/dir and SIGHUP the app.
package reload

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Run watches watchPath (its dir, since ConfigMap updates swap the ..data symlink) and, on change,
// SIGHUPs the process whose /proc/<pid>/cmdline contains procMatch. Blocks. Needs shareProcessNamespace.
func Run(watchPath, procMatch string) error {
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

// findPID scans /proc for a process whose cmdline contains match (using cmdline, not comm,
// which truncates to 15 chars). Excludes self and autoconfig.
func findPID(match string) int {
	procs, _ := filepath.Glob("/proc/[0-9]*")
	self := os.Getpid()
	for _, d := range procs {
		b, err := os.ReadFile(filepath.Join(d, "cmdline"))
		if err != nil {
			continue
		}
		cmd := strings.ReplaceAll(string(b), "\x00", " ")
		if strings.Contains(cmd, match) && !strings.Contains(cmd, "autoconfig") {
			var pid int
			if _, err := fmt.Sscanf(filepath.Base(d), "%d", &pid); err == nil && pid != self {
				return pid
			}
		}
	}
	return 0
}
