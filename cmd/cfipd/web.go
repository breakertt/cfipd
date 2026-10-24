package main

import (
	_ "embed"
	"net/http"
)

//go:embed web.html
var webPage []byte

// webAPI whitelists the read-only ctl paths exposed over HTTP.
// Mutation endpoints (/promote, /quarantine, /scan, /add) are never reachable here.
var webAPI = map[string]string{
	"/api/status":   "/status",
	"/api/standbys": "/standbys",
	"/api/relay":    "/relay",
}

// serveWeb runs the read-only dashboard. It talks to the daemon exclusively
// through the unix socket ctl interface, so it never touches daemon state
// directly and can run as a separate process.
func serveWeb(sock, listen string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(webPage)
	})
	for api, ctl := range webAPI {
		mux.HandleFunc("GET "+api, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			raw, err := ctlGet(sock, ctl, "")
			if err != nil {
				http.Error(w, "daemon unreachable: "+err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(raw)
		})
	}
	return http.ListenAndServe(listen, mux)
}
