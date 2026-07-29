// Package hagate 是 master-standby 用的 leader 选举门控:2 副本里只有持 Lease 的 leader
// 让 /healthz 返 200(且本地 app 探活通过),standby 返 503 → 只有 leader 进 Service endpoints。
// leader 挂了 Lease 到期(~LeaseDuration),standby 接管变 leader → 自动 failover。
package hagate

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Run 启动 leader 选举 + /healthz。leaseName/ns 定位 Lease,id 是本 pod 身份(POD_NAME)。
// httpAddr 是健康端口(如 :8081);appTCP 非空时 /healthz 还要求本地 app 端口可连(host:port)。
func Run(leaseName, ns, id, httpAddr, appTCP string) error {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	var leader atomic.Bool
	// readiness:leader 且(未配 appTCP 或 app 端口可连)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !leader.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("standby"))
			return
		}
		if appTCP != "" && !dialOK(appTCP) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("leader-but-app-down"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("leader"))
	})
	go func() {
		if err := http.ListenAndServe(httpAddr, nil); err != nil {
			log.Fatalf("healthz listen %s: %v", httpAddr, err)
		}
	}()

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: ns},
		Client:     cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}
	// 循环:丢主后回到候选态继续抢,不退出(pod 不重启,变 standby 热备)。
	for {
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
		time.Sleep(2 * time.Second) // 短暂退避后重新竞选
	}
}

func dialOK(hostport string) bool {
	c, err := net.DialTimeout("tcp", hostport, 1500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}
