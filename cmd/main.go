// autoconfig controller: watch ModelRoute (routing.modelsphere.dev/v1alpha1) → 发现后端 →
// 写 openresty peers / CART workers / monitor config。
// (reload sidecar 是独立程序,见 cmd/reload。)
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

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
		LeaderElectionID:        "autoconfig-controller.routing.modelsphere.dev",
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
	// ModelFleet: aggregates several single-hardware ModelRoutes into one model behind one
	// gateway address. It only reads ModelRoute status, so the two reconcilers share nothing
	// but the API group.
	//
	// Registered only when its CRD is actually installed. `helm upgrade` does not apply
	// crds/, so upgrading an existing release without applying the ModelFleet CRD by hand
	// leaves the type missing - and watching a type that does not exist fails the manager,
	// which would take ModelRoute down with it. A cluster that never asked for ModelFleet
	// must not lose openresty peers, CART workers and monitor.conf over it.
	served, err := servedKinds(cs)
	if err != nil {
		return err
	}
	if served["ModelFleet"] {
		if err := (&controller.ModelFleetReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			return err
		}
	} else {
		log.Print("ModelFleet CRD not installed: skipping that reconciler " +
			"(apply deploy/helm/autoconfig/crds/ and restart to enable it)")
	}
	// APIConsumer: who may call the gateway, and with which keys. Gated on its own CRD for
	// the same reason - a cluster that only uses ModelRoute must not lose openresty peers
	// over a type it never asked for.
	if served["APIConsumer"] {
		consumers := &controller.APIConsumerReconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
		}
		if err := consumers.SetupWithManager(mgr); err != nil {
			return err
		}
		// ...and a watch on the gateways' key-auth plugin itself. The gateway chart no
		// longer renders defaultConfig.consumers, so this controller is the only thing that
		// can create it - and key_auth cannot parse a defaultConfig without it (FAIL_CLOSE:
		// the gateway refuses everything). A fresh install has zero APIConsumers, so the
		// APIConsumer watch would never fire; this one does, and the event also says which
		// namespace, so no default has to be configured or guessed.
		if err := (&controller.KeyAuthReconciler{APIConsumerReconciler: consumers}).
			SetupWithManager(mgr); err != nil {
			return err
		}
	} else {
		log.Print("APIConsumer CRD not installed: skipping that reconciler " +
			"(apply deploy/helm/autoconfig/crds/ and restart to enable it)")
	}
	// controller-runtime 自带的 ping 检查:进程存活即通过。
	// 注意它【不能】发现 reconcile 卡死 —— 要那个得看 metrics 的 workqueue_depth。
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	extra := ""
	for _, k := range []string{"ModelFleet", "APIConsumer"} {
		if served[k] {
			extra += " + " + k
		}
	}
	log.Printf("controller start: ModelRoute%s (routing.modelsphere.dev/v1alpha1)", extra)
	return mgr.Start(ctrl.SetupSignalHandler())
}

// servedKinds asks the API server which kinds of our group it serves. Discovery rather than
// a Get on each CRD object: it needs no apiextensions permission, and "is this kind served"
// is exactly the question. One call for every optional reconciler.
func servedKinds(cs kubernetes.Interface) (map[string]bool, error) {
	out := map[string]bool{}
	list, err := cs.Discovery().ServerResourcesForGroupVersion(routingv1.GroupVersion.String())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return out, nil // the whole group is absent: ModelRoute's CRD is missing too
		}
		return nil, fmt.Errorf("discover %s: %w", routingv1.GroupVersion, err)
	}
	for _, r := range list.APIResources {
		out[r.Kind] = true
	}
	return out, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
