package main

import (
	"net/http"
	"net/http/httptest"
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

// With web_password set, every path needs that password under any user name.
// Without it, the dashboard stays open.
func TestWebPassword(t *testing.T) {
	get := func(h http.Handler, path string, auth func(*http.Request)) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		if auth != nil {
			auth(r)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	basic := func(user, pass string) func(*http.Request) {
		return func(r *http.Request) { r.SetBasicAuth(user, pass) }
	}
	h := webHandler("unused.sock", "s3cret")
	for _, tc := range []struct {
		path string
		auth func(*http.Request)
		want int
	}{
		{"/", nil, 401},
		{"/api/status", nil, 401},
		{"/", basic("", "wrong"), 401},
		{"/", basic("", "s3cret"), 200},
		{"/", basic("anyone", "s3cret"), 200},
	} {
		w := get(h, tc.path, tc.auth)
		if w.Code != tc.want {
			t.Errorf("%s: got %d, want %d", tc.path, w.Code, tc.want)
		}
		if w.Code == 401 && w.Header().Get("WWW-Authenticate") != `Basic realm="cfipd"` {
			t.Errorf("%s: 401 without a Basic challenge", tc.path)
		}
	}
	if w := get(webHandler("unused.sock", ""), "/", nil); w.Code != 200 {
		t.Errorf("no password: got %d, want 200", w.Code)
	}
}
