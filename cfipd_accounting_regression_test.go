package main

import (
	"context"
	"testing"
	"time"
)

func TestRelayConnectionsAreNotSyntheticProbeUsage(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.0.1", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.pool.ips[ip.Addr] = ip
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: okProbe(10, "")})
	if len(d.dayCounts) != 0 {
		t.Fatal("customer TCP connection must not consume synthetic probe counters")
	}
	if d.relayStat(ip.Addr).Total != 1 {
		t.Fatal("real connection accounting must remain intact")
	}
}

func TestIdleWorkerStopsOnCancellation(t *testing.T) {
	d := testDaemon(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.worker(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		d.jobs <- job{} // release the pre-fix idle receive so the test leaves no worker
		t.Fatal("an idle worker must stop without receiving another job")
	}
}
