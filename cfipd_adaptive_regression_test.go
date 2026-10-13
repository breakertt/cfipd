package main

import (
	"testing"
	"time"
)

func TestConfirmedSlowAvailableNodeCanReduceProbeCadence(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.0.1", Role: RoleActive, State: StateDegraded, Colo: "NRT", Baseline: 100, DegradedSince: t0}
	d.pool.ips[ip.Addr] = ip
	// This node remains slow relative to its former baseline, but repeated
	// checks confirm that it is available and no longer changing. Keeping the
	// degraded label must not force endless fast probes without a substitute.
	for i := range 30 {
		d.pool.applyHealth(ip, okProbe(500, "NRT"), t0.Add(time.Duration(i)*10*time.Second))
	}
	if ip.State == StateHealthy {
		t.Fatal("stable-but-still-slow must not be called fully recovered")
	}
	if every := ip.healthEvery(d.cfg); every <= 30*time.Second {
		t.Fatalf("confirmed available and stable latency must back off even while quality is degraded, interval=%s", every)
	}
}
