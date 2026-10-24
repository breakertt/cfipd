package main

import (
	"net/url"
	"regexp"
	"strings"
	"testing"
)

// A reverse proxy can mount the dashboard under a subpath and strip the
// prefix. Every API request of the page must then stay under that subpath.
func TestWebPageAPIPathsWorkUnderSubpath(t *testing.T) {
	base, _ := url.Parse("https://example.test/cfipd/")
	found := map[string]bool{}
	for _, m := range regexp.MustCompile(`['"](/?api/[a-z]+)['"]`).FindAllStringSubmatch(string(webPage), -1) {
		got := base.ResolveReference(&url.URL{Path: m[1]}).Path
		if !strings.HasPrefix(got, "/cfipd/") {
			t.Errorf("page requests %q, which resolves to %q outside the subpath", m[1], got)
		}
		found["/"+strings.TrimPrefix(m[1], "/")] = true
	}
	for api := range webAPI {
		if !found[api] {
			t.Errorf("page does not request %s", api)
		}
	}
}
