package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A download that sends headers and then no body byte before the timeout is a
// failure, not a 0 Mbps sample: a 0 would score as an active without samples.
func TestSpeedTestWithoutDataFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	cfg := defaultConfig()
	cfg.DownURL = srv.URL + "/__down"
	cfg.SpeedTimeout = Duration{200 * time.Millisecond}
	if r := speedTest(context.Background(), cfg, "127.0.0.1"); r.Err == "" {
		t.Fatalf("a download without data must fail, got %+v", r)
	}
}
