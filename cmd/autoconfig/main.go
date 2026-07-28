// autoconfig: k8s backend discovery -> openresty/CART config.
//
//	agent mode:  autoconfig --config /etc/autoconfig/config.yaml
//	reload mode: autoconfig --reload-mode --watch /cfg/config.yaml --process cache-aware-router
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"autoconfig/internal/agent"
	"autoconfig/internal/config"
	"autoconfig/internal/reload"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[autoconfig] ")

	reloadMode := flag.Bool("reload-mode", false, "run as reload sidecar (watch file -> SIGHUP)")
	watch := flag.String("watch", envOr("PS_WATCH", ""), "reload mode: file/dir to watch")
	proc := flag.String("process", envOr("PS_PROCESS", ""), "reload mode: process cmdline substring to SIGHUP")
	cfgPath := flag.String("config", envOr("PS_CONFIG", "/etc/autoconfig/config.yaml"), "agent mode: config path")
	flag.Parse()

	if *reloadMode {
		if err := reload.Run(*watch, *proc); err != nil {
			log.Fatalf("reload: %v", err)
		}
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
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
	if err := agent.Run(ctx, cs, cfg); err != nil && err != context.Canceled {
		log.Fatalf("agent: %v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
