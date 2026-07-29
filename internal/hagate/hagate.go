// Package hagate 是 master-standby 用的 leader 选举 + 标签门控:2 副本都保持 Ready(健康),
// 但只有持 Lease 的 leader 给自己打 active 标签(且本地 app 端口可连);Service selector 匹配该标签
// → 只有 leader 进 endpoints。用标签而非 readiness 门控,避免 standby 永久 NotReady 卡住 Deployment 滚动。
// leader 挂了 Lease 到期(~LeaseDuration),standby 接管打标签 → 自动 failover。
package hagate

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Run 启动 leader 选举 + 标签门控。leaseName/ns 定位 Lease,id 是本 pod(POD_NAME)。
// labelKey/labelVal 是 active 标签(Service selector 匹配它);appTCP 非空时还要求本地端口可连才打标签。
// httpAddr 暴露 /healthz(200=leader,供调试/liveness)。
func Run(leaseName, ns, id, httpAddr, appTCP, labelKey, labelVal string) error {
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
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Fatalf("healthz listen %s: %v", httpAddr, err)
		}
	}()

	// 标签协调:desired = leader 且(未配 appTCP 或端口可连);与当前标签不符就 patch。
	go func() {
		have := false
		for {
			want := leader.Load() && (appTCP == "" || dialOK(appTCP))
			if want != have {
				if err := setPodLabel(cs, ns, id, labelKey, labelVal, want); err != nil {
					log.Printf("set label %s=%v: %v", labelKey, want, err)
				} else {
					have = want
					log.Printf("pod %s active=%v", id, want)
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: ns},
		Client:     cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}
	for { // 丢主后回到候选态继续抢,不退出(pod 不重启,变 standby 热备)
		leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { leader.Store(true); log.Printf("became leader (%s)", id) },
				OnStoppedLeading: func() { leader.Store(false); log.Printf("lost leadership (%s)", id) },
			},
		})
		leader.Store(false)
		time.Sleep(2 * time.Second)
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
