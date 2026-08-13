// Package hagate 是 master-standby 用的 leader 选举 + 标签门控:2 副本都保持 Ready(健康),
// 但只有持 Lease 的 leader 给自己打 active 标签(且本地 app 端口可连);Service selector 匹配该标签
// → 只有 leader 进 endpoints。用标签而非 readiness 门控,避免 standby 永久 NotReady 卡住 Deployment 滚动。
//
// failover:
//   - 计划内(SIGTERM:删 pod/滚动/驱逐):捕获信号 → 取消 context → ReleaseOnCancel 释放 Lease
//     → standby ~1-2s(RetryPeriod)接管【新】流量。本 pod【保留】active 标签:此时它已是
//     terminating endpoint(deletionTimestamp),Cilium graceful-terminating 把新连接导向 standby、
//     老在途连接留在本 pod 直到排空(openresty SIGQUIT + grace),避免在途被 reset;pod 退出即自动出 endpoints。
//   - 存活丢主(Lease 续约失败,pod 没死):非 terminating → 协调循环摘标签,离开 Service(避免 2-active)。
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
	Lease, Namespace, Identity                string
	HTTPAddr, AppTCP                          string
	LabelKey, LabelVal                        string
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

	// terminating:收到 SIGTERM(=pod 正被删除)后置真,永久强制 want=true —— 保留 active 标签,
	// 让本 pod 作为 terminating endpoint 留在 Service,由 Cilium graceful-terminating 优雅排空在途连接。
	var terminating atomic.Bool

	// 信号 → 取消 ctx(RunOrDie 收到后 ReleaseOnCancel 释放 Lease)
	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigc
		log.Printf("SIGTERM: releasing lease, KEEPING active label for graceful drain (%s)", c.Identity)
		terminating.Store(true) // 强制 want=true:保留标签,不硬摘(硬摘=从 Service selector 剔除 → Cilium reset 在途连接)
		cancel()                // 释放 Lease → standby 接管新流量;本 pod 保留标签作 terminating endpoint 排空在途
	}()

	// 标签协调(level-triggered):每轮读 pod 【实际】标签,与 desired(leader 且 appTCP 可连)不符就纠正。
	// 不用内部 have 猜 —— 标签被外部弄掉(手删/别的东西改)也能自愈,不会「以为 active 其实没标签→零端点断流」。
	go func() {
		reconcile := func(want bool) {
			actual, err := podHasLabel(cs, c.Namespace, c.Identity, c.LabelKey, c.LabelVal)
			if err != nil {
				log.Printf("read pod label: %v", err)
				return
			}
			if actual != want {
				if err := setPodLabel(cs, c.Namespace, c.Identity, c.LabelKey, c.LabelVal, want); err != nil {
					log.Printf("set label %s=%v: %v", c.LabelKey, want, err)
				} else {
					log.Printf("pod %s active=%v (reconciled, actual was %v)", c.Identity, want, actual)
				}
			}
		}
		for {
			// terminating 后恒 true:强制保留标签。即使这轮 want 在信号前算出(TOCTOU),ctx.Done 分支
			// 退出前会再确保标签在 —— 循环最后一个写动作是「保留」,让 pod 以 terminating endpoint 排空。
			reconcile(terminating.Load() || (leader.Load() && (c.AppTCP == "" || dialOK(c.AppTCP))))
			select {
			case <-ctx.Done():
				reconcile(true) // 退出前保留 active 标签:pod 仍 terminating,靠 Cilium graceful 排空在途;pod 退出即自动出 endpoints
				return
			case <-time.After(2 * time.Second):
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

// podHasLabel 读 pod 当前是否带 key=val 的标签(level-triggered 协调用)。
func podHasLabel(cs kubernetes.Interface, ns, pod, key, val string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := cs.CoreV1().Pods(ns).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return p.Labels[key] == val, nil
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
