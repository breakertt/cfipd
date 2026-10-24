package main

import "testing"

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
