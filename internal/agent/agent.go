// Package agent runs the central discovery loop: discover per target, render per sink, write ConfigMaps.
package agent

import (
	"context"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"autoconfig/internal/config"
	"autoconfig/internal/discovery"
	"autoconfig/internal/sink"
)

func Run(ctx context.Context, cs kubernetes.Interface, cfg *config.Config) error {
	var sinks []sink.Sink
	for _, s := range cfg.Sinks {
		sk, err := sink.New(s)
		if err != nil {
			return err
		}
		sinks = append(sinks, sk)
	}

	lastPeers := map[string][]config.Peer{}    // target -> last non-empty peers (fail-safe)
	lastRendered := map[string]sink.Rendered{} // sink.Name() -> last written data

	interval := time.Duration(cfg.IntervalSeconds) * time.Second
	log.Printf("[agent] start: %d targets, %d sinks, interval %s", len(cfg.Targets), len(sinks), interval)

	for {
		// 1) discover per target (fail-safe: keep last on error/empty)
		peersByTarget := map[string][]config.Peer{}
		for _, t := range cfg.Targets {
			discovered, err := discovery.Discover(ctx, cs, t)
			if err != nil {
				log.Printf("[agent] discover %q failed: %v (keep last)", t.Name, err)
				peersByTarget[t.Name] = lastPeers[t.Name]
				continue
			}
			if len(discovered) == 0 {
				log.Printf("[agent] target %q: 0 ready backends (keep last, fail-safe)", t.Name)
				peersByTarget[t.Name] = lastPeers[t.Name]
				continue
			}
			full := append(append([]config.Peer{}, t.StaticPeers...), discovered...)
			peersByTarget[t.Name] = full
			lastPeers[t.Name] = full
		}

		// 2) render + write per sink (only if changed)
		for _, sk := range sinks {
			rendered, err := sk.Render(peersByTarget)
			if err != nil {
				log.Printf("[agent] render %s failed: %v", sk.Name(), err)
				continue
			}
			if equalRendered(lastRendered[sk.Name()], rendered) {
				continue // no change -> don't touch ConfigMap (avoids needless reload)
			}
			if err := writeConfigMap(ctx, cs, sk.ConfigMapNS(), sk.ConfigMapName(), rendered); err != nil {
				log.Printf("[agent] write ConfigMap %s/%s (%s) failed: %v",
					sk.ConfigMapNS(), sk.ConfigMapName(), sk.Name(), err)
				continue
			}
			lastRendered[sk.Name()] = rendered
			log.Printf("[agent] updated ConfigMap %s/%s (%s)", sk.ConfigMapNS(), sk.ConfigMapName(), sk.Name())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// writeConfigMap merges rendered keys into the ConfigMap (preserving other keys), creating if absent.
func writeConfigMap(ctx context.Context, cs kubernetes.Interface, ns, name string, data sink.Rendered) error {
	cms := cs.CoreV1().ConfigMaps(ns)
	cm, err := cms.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Data:       map[string]string{},
		}
		for k, v := range data {
			cm.Data[k] = v
		}
		_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range data {
		cm.Data[k] = v
	}
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

func equalRendered(a, b sink.Rendered) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
