package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// TestManualPromoteWaitsForLongJob: a manual promotion must not start while
// another long job may be downloading.
func TestManualPromoteWaitsForLongJob(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	d.pool.cooldown(d.pool.ips["198.18.0.1"], "fixture failure", t0) // open an active slot
	d.rechecking = true
	req := ctlRequest{path: "/promote", arg: sb.Addr, reply: make(chan ctlReply, 1)}
	d.handleCtl(req)
	if rep := <-req.reply; rep.err == "" || d.promoting {
		t.Fatalf("manual promote must wait for the running recheck, got %+v", rep)
	}
}

// Every path that the dashboard reads must reach the event loop through the
// ctl socket.
func TestCtlServesWebAPIPaths(t *testing.T) {
	d := testDaemon(t)
	d.cfg.Socket = filepath.Join(t.TempDir(), "ctl.sock")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d.ctx = ctx
	if err := serveCtl(d); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			select {
			case req := <-d.ctl:
				d.handleCtl(req)
			case <-ctx.Done():
				return
			}
		}
	}()
	for api, path := range webAPI {
		if _, err := ctlGet(d.cfg.Socket, path, ""); err != nil {
			t.Errorf("%s -> ctl %s: %v", api, path, err)
		}
	}
}

// A ctl reply is encoded on the HTTP goroutine while the event loop runs on,
// so it must not share maps or structs with loop state: a shared day-count
// map crashed the daemon with "concurrent map iteration and map write".
func TestCtlRepliesShareNoLoopState(t *testing.T) {
	d := testDaemon(t)
	d.dayCounts["discover"] = 1
	d.scan.stats["198.18.0"] = &PrefixStat{Prefix: "198.18.0", Probes: 1}
	ask := func(path string) any {
		req := ctlRequest{path: path, reply: make(chan ctlReply, 1)}
		d.handleCtl(req)
		return (<-req.reply).v
	}
	status := ask("/status").(map[string]any)
	prefixes := ask("/prefixes")
	d.dayCounts["discover"] = 2 // the loop goes on
	d.scan.stats["198.18.0"].Probes = 2
	if got := fmt.Sprint(status["probes_today"]); got != "map[discover:1]" {
		t.Errorf("status shares the day counts with the loop: %s", got)
	}
	if got := fmt.Sprint(prefixes); !strings.Contains(got, "198.18.0 1 ") {
		t.Errorf("prefixes share statistics with the loop: %s", got)
	}
}
