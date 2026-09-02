// autoconfig controller: watch ModelRoute (routing.gpucluster.io/v1alpha1) → 发现后端 →
// 写 openresty peers / CART workers / monitor config。
// (reload sidecar 是独立程序,见 cmd/reload。)
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	slov1 "autoconfig/api/inference/v1alpha1"
	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/controller"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[autoconfig] ")

	leaderElect := flag.Bool("leader-elect", true, "enable leader election (HA)")
	flag.Parse()

	if err := runController(*leaderElect); err != nil {
		log.Fatalf("controller: %v", err)
	}
}

func runController(leaderElect bool) error {
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(routingv1.AddToScheme(scheme))
	// LLMSLORequirement 是别人的 CRD,我们只读(翻译成 openresty route conf 里的
	// ttft_metrics / tps_metrics,与 peers 走同一条 ConfigMap + reload 通道)。
	utilruntime.Must(slov1.AddToScheme(scheme))

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	shutdownTimeout := 10 * time.Second
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		// metrics server:controller-runtime 自带的 /metrics(reconcile 次数/耗时/错误、
		// workqueue 深度与未完成时长、client-go 对 apiserver 的请求、Go 运行时)。
		// 关键用途:workqueue_unfinished_work_seconds 持续上涨 = reconcile 卡死
		// (如 cart ConfigMap wedge)—— 这是 healthz.Ping 发现不了的,它只证明进程能应答。
		// 端口 8080 是 kubebuilder 惯例;本 pod 单容器、非 hostNetwork,不存在占用问题
		// (早先注释说的"避免端口占用"是 kubebuilder 老默认值撞车的历史问题)。
		Metrics: metricsserver.Options{BindAddress: ":8080"},
		// 健康探针端口:kubebuilder 默认 8081。之前没开,导致 Deployment 里连 liveness 都没法配 ——
		// 一旦 reconcile 卡死(如 cart ConfigMap wedge),进程活着但不干活,k8s 永远不会重启它。
		HealthProbeBindAddress:  ":8081",
		LeaderElection:          leaderElect,
		LeaderElectionID:        "autoconfig-controller.routing.gpucluster.io",
		LeaderElectionNamespace: envOr("PS_NAMESPACE", ""), // 空 = in-cluster 自动推断
		GracefulShutdownTimeout: &shutdownTimeout,          // SIGTERM 后最多等 10s 收 runnable → pod 及时终止
	})
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	if err := (&controller.ModelRouteReconciler{
		Client:    mgr.GetClient(),
		Clientset: cs,
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return err
	}
	// controller-runtime 自带的 ping 检查:进程存活即通过。
	// 注意它【不能】发现 reconcile 卡死 —— 要那个得看 metrics 的 workqueue_depth。
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	log.Print("controller start: ModelRoute (routing.gpucluster.io/v1alpha1)")
	return mgr.Start(ctrl.SetupSignalHandler())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
