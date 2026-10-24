package main

import (
	"context"
	"path/filepath"
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
