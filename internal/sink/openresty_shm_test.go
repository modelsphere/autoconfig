package sink

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// nginx carves a shared zone into pages of the kernel page size. arm64 hosts often run 64 KiB
// pages, where a zone of 128k or less fails at startup with "ngx_slab_alloc() failed: no memory"
// (measured on Kunpeng 920: 128k fails, 192k starts). A route that renders such a dict takes the
// whole openresty down on those hosts, so none may be under 256k.
func TestRenderRoute_SharedDictsFitLargePages(t *testing.T) {
	got, err := RenderRoute(llmData(map[string]string{"tps_limit_tps": "30"}))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	re := regexp.MustCompile(`(?m)^\s*lua_shared_dict\s+(\S+)\s+(\d+)([kKmM]?)\s*;`)
	dicts := re.FindAllStringSubmatch(got, -1)
	if len(dicts) == 0 {
		t.Fatalf("no lua_shared_dict found in the rendered route -- the check would pass on nothing:\n%s", got)
	}
	for _, d := range dicts {
		n, _ := strconv.Atoi(d[2])
		kib := n / 1024 // bytes
		switch strings.ToLower(d[3]) {
		case "k":
			kib = n
		case "m":
			kib = n * 1024
		}
		if kib < 256 {
			t.Errorf("lua_shared_dict %s is %s%s: under 256k it fails to start on 64 KiB-page hosts", d[1], d[2], d[3])
		}
	}
}
