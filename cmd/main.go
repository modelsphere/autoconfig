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
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	shutdownTimeout := 10 * time.Second
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"}, // 关 metrics server,避免端口占用
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
	log.Print("controller start: ModelRoute (routing.gpucluster.io/v1alpha1)")
	return mgr.Start(ctrl.SetupSignalHandler())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
