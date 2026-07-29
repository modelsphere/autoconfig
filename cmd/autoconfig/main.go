// autoconfig: k8s backend discovery -> openresty/CART config.
//
//	agent mode:      autoconfig --config /etc/autoconfig/config.yaml   (ConfigMap 驱动)
//	controller mode: autoconfig --controller                          (CRD ModelRoute 驱动)
//	reload mode:     autoconfig --reload-mode --watch /cfg --process cache-aware-router
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/agent"
	"autoconfig/internal/controller"
	"autoconfig/internal/reload"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[autoconfig] ")

	reloadMode := flag.Bool("reload-mode", false, "run as reload sidecar (watch file -> SIGHUP)")
	controllerMode := flag.Bool("controller", false, "run as CRD controller (ModelRoute)")
	watch := flag.String("watch", envOr("PS_WATCH", ""), "reload mode: file/dir to watch")
	proc := flag.String("process", envOr("PS_PROCESS", ""), "reload mode: process cmdline substring to SIGHUP")
	cfgPath := flag.String("config", envOr("PS_CONFIG", "/etc/autoconfig/config.yaml"), "agent mode: config path")
	leaderElect := flag.Bool("leader-elect", true, "controller mode: enable leader election (HA)")
	flag.Parse()

	if *reloadMode {
		if err := reload.Run(*watch, *proc); err != nil {
			log.Fatalf("reload: %v", err)
		}
		return
	}

	if *controllerMode {
		if err := runController(*leaderElect); err != nil {
			log.Fatalf("controller: %v", err)
		}
		return
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("in-cluster config: %v", err)
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("clientset: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := agent.Run(ctx, cs, *cfgPath); err != nil && err != context.Canceled {
		log.Fatalf("agent: %v", err)
	}
}

// runController 启动 controller-runtime manager,调谐 ModelRoute。
func runController(leaderElect bool) error {
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(routingv1.AddToScheme(scheme))

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: "0"}, // 关 metrics server,避免端口占用
		LeaderElection:          leaderElect,
		LeaderElectionID:        "autoconfig-controller.routing.4pd.io",
		LeaderElectionNamespace: envOr("PS_NAMESPACE", ""), // 空 = in-cluster 自动推断
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
	log.Print("controller start: ModelRoute (routing.4pd.io/v1alpha1)")
	return mgr.Start(ctrl.SetupSignalHandler())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
