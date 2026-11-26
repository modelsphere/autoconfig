package main

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// A cluster without Higress must not stop this binary from starting. The key-auth watch is a
// For() watch, so its informer is built at startup and blocks on the cache syncing: register
// it where the kind is not served and the manager exits when CacheSyncTimeout runs out, the
// pod crash-loops, and ModelRoute goes down with it - openresty peers, CART workers,
// monitor.conf. Most clusters are in that state, since enabled.higress is false by default
// while this release ships the APIConsumer CRD everywhere.
//
// So "is WasmPlugin served" has to be answerable, and a missing group has to be an answer
// rather than an error.
func TestKindServed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resources []*metav1.APIResourceList
		want      bool
	}{
		{
			name: "higress installed",
			resources: []*metav1.APIResourceList{{
				GroupVersion: wasmPluginGroupVersion,
				APIResources: []metav1.APIResource{{Kind: "WasmPlugin"}, {Kind: "McpBridge"}},
			}},
			want: true,
		},
		{
			// The common case: no Higress at all. Must come back false, not an error.
			name:      "group absent",
			resources: nil,
			want:      false,
		},
		{
			// The group exists but not this kind - an older or trimmed Higress.
			name: "group without the kind",
			resources: []*metav1.APIResourceList{{
				GroupVersion: wasmPluginGroupVersion,
				APIResources: []metav1.APIResource{{Kind: "McpBridge"}},
			}},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := fake.NewSimpleClientset()
			cs.Discovery().(*fakediscovery.FakeDiscovery).Resources = tc.resources
			got, err := kindServed(cs, wasmPluginGroupVersion, "WasmPlugin")
			if err != nil {
				t.Fatalf("a cluster without Higress must not be an error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("kindServed = %v, want %v", got, tc.want)
			}
		})
	}
}

// The gate itself. Trivial today, and separate because what it decides is not: the key-auth
// watch is a For(), so its informer is built at startup and waited on, and registering it
// where Higress is absent makes the manager exit - crash-looping the pod and taking
// ModelRoute down with it. If this ever grows a second condition, it has a place to go and a
// test that says what it is for.
func TestKeyAuthWatchWanted(t *testing.T) {
	if keyAuthWatchWanted(false) {
		t.Fatal("the key-auth watch would be registered on a cluster without Higress")
	}
	if !keyAuthWatchWanted(true) {
		t.Fatal("the key-auth watch would be skipped on a cluster that has Higress - then " +
			"nothing creates key-auth's consumers key and the gateway stays FAIL_CLOSE")
	}
}
