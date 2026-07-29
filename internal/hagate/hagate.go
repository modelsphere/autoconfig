// Package hagate 是 master-standby 用的 leader 选举 + 标签门控:2 副本都保持 Ready(健康),
// 但只有持 Lease 的 leader 给自己打 active 标签(且本地 app 端口可连);Service selector 匹配该标签
// → 只有 leader 进 endpoints。用标签而非 readiness 门控,避免 standby 永久 NotReady 卡住 Deployment 滚动。
//
// failover:
//   - 计划内(SIGTERM:删 pod/滚动/驱逐):捕获信号 → 取消 context → ReleaseOnCancel 主动释放 Lease
//     + 摘掉自己 active 标签 → standby ~1-2s(RetryPeriod)接管,近乎无缝。
//   - 硬崩(节点宕/kill -9):无从释放,standby 等 Lease 过期(~LeaseDuration)接管。
package hagate

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Config 是 hagate 的运行参数。
type Config struct {
	Lease, Namespace, Identity string
	HTTPAddr, AppTCP           string
	LabelKey, LabelVal         string
	LeaseDuration, RenewDeadline, RetryPeriod time.Duration
}

// Run 启动 leader 选举 + 标签门控,阻塞直到收到 SIGTERM/SIGINT(优雅释放后退出)。
func Run(c Config) error {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	var leader atomic.Bool
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if leader.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("leader"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("standby"))
	})
	go func() {
		if err := http.ListenAndServe(c.HTTPAddr, nil); err != nil {
			log.Fatalf("healthz listen %s: %v", c.HTTPAddr, err)
		}
	}()

	// 信号 → 取消 ctx(RunOrDie 收到后 ReleaseOnCancel 释放 Lease)
	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigc
		log.Printf("signal received, releasing lease + label (%s)", c.Identity)
		_ = setPodLabel(cs, c.Namespace, c.Identity, c.LabelKey, c.LabelVal, false) // 先摘标签,Service 立刻剔除本 pod
		cancel()
	}()

	// 标签协调:desired = leader 且(未配 appTCP 或端口可连);与当前标签不符就 patch。
	go func() {
		have := false
		for {
			want := leader.Load() && (c.AppTCP == "" || dialOK(c.AppTCP))
			if want != have {
				if err := setPodLabel(cs, c.Namespace, c.Identity, c.LabelKey, c.LabelVal, want); err != nil {
					log.Printf("set label %s=%v: %v", c.LabelKey, want, err)
				} else {
					have = want
					log.Printf("pod %s active=%v", c.Identity, want)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: c.Lease, Namespace: c.Namespace},
		Client:     cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: c.Identity},
	}
	for { // 丢主后回到候选态继续抢;收到信号(ctx 取消)则退出
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true, // ctx 取消时把 Lease 释放掉 → standby 立刻可抢
			LeaseDuration:   c.LeaseDuration,
			RenewDeadline:   c.RenewDeadline,
			RetryPeriod:     c.RetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { leader.Store(true); log.Printf("became leader (%s)", c.Identity) },
				OnStoppedLeading: func() { leader.Store(false); log.Printf("lost leadership (%s)", c.Identity) },
			},
		})
		leader.Store(false)
		if ctx.Err() != nil { // 收到信号,优雅退出
			log.Printf("shutting down (%s)", c.Identity)
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

// setPodLabel 用 merge-patch 加/删自己 pod 的一个 label(删=置 null)。
func setPodLabel(cs kubernetes.Interface, ns, pod, key, val string, want bool) error {
	var v interface{}
	if want {
		v = val
	} else {
		v = nil // merge-patch 里 null 删除该 key
	}
	patch, _ := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{key: v}}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cs.CoreV1().Pods(ns).Patch(ctx, pod, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func dialOK(hostport string) bool {
	c, err := net.DialTimeout("tcp", hostport, 1500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}
