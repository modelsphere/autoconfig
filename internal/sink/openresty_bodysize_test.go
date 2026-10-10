package sink

import (
	"strings"
	"testing"

	"autoconfig/internal/config"
)

func llmData(extra map[string]string) RouteData {
	return RouteData{Route: "glm", Peers: []config.Peer{{IP: "10.0.0.1", Port: 8000, Name: "backend-0"}}, Extra: extra}
}

// Without max_body_size the route renders no client_max_body_size: it inherits the engine's http-level
// default, and an older openresty image (which still sets the directive in router_locations.inc) is
// unaffected -- a second one in the same server would fail nginx -t.
func TestRenderRoute_LLMNoMaxBodySizeByDefault(t *testing.T) {
	got, err := RenderRoute(llmData(map[string]string{"tps_limit_tps": "30"}))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(got, "client_max_body_size") {
		t.Errorf("no max_body_size set, but the route renders client_max_body_size:\n%s", got)
	}
}

// max_body_size becomes the server's client_max_body_size, before the shared include (directives in
// the include must not repeat it), and is not passed into the lua route table.
func TestRenderRoute_LLMMaxBodySize(t *testing.T) {
	got, err := RenderRoute(llmData(map[string]string{"max_body_size": " 50m ", "tps_limit_tps": "30"}))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	dir := strings.Index(got, "client_max_body_size 50m;")
	inc := strings.Index(got, "include conf.d/router_locations.inc;")
	if dir < 0 || inc < 0 || dir > inc {
		t.Fatalf("want client_max_body_size 50m; before the include (dir=%d inc=%d):\n%s", dir, inc, got)
	}
	if strings.Contains(got, "max_body_size =") {
		t.Errorf("max_body_size leaked into the lua route table:\n%s", got)
	}
	if !strings.Contains(got, "tps_limit_tps = 30") {
		t.Errorf("other values must still reach the lua table:\n%s", got)
	}
}

// A value nginx would not accept is an error, not a rendered conf: nginx -t failing stops the reload
// for every route on that openresty.
func TestRenderRoute_LLMMaxBodySizeRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"50 MB", "50mb", "1.5m", "", "-1m", "50m;"} {
		if _, err := RenderRoute(llmData(map[string]string{"max_body_size": bad})); err == nil {
			t.Errorf("max_body_size %q: want an error", bad)
		}
	}
	for _, good := range []string{"10m", "64M", "1024k", "1g", "1048576"} {
		if _, err := RenderRoute(llmData(map[string]string{"max_body_size": good})); err != nil {
			t.Errorf("max_body_size %q: %v", good, err)
		}
	}
}
