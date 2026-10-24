package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func testDaemon(t *testing.T) *daemon {
	t.Helper()
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	d := newDaemon(defaultConfig(), st)
	d.ctx = context.Background() // unit tests drive apply/schedule without run()
	return d
}

func okProbe(rtt float64, colo string) ProbeResult {
	return ProbeResult{OK: true, Totalms: rtt, Colo: colo}
}

// restrictedProbe is what probe() returns when the candidate IP answers
// Cloudflare error 1034 for the zone probe host.
func restrictedProbe() ProbeResult {
	return ProbeResult{Hard: true, Restricted: true, Colo: "NRT", Err: "zone example.com: error 1034 (edge IP restricted)"}
}

// pkPairFixture is a full active pool whose worst active is the incumbent,
// plus a credible challenger standby. These fixtures describe behavioral
// contracts, not production traffic.
func pkPairFixture(t *testing.T) (*daemon, *IP, *IP) {
	t.Helper()
	d := testDaemon(t)
	for i, colo := range []string{"NRT", "SIN", "HKG"} {
		addr := fmt.Sprintf("198.18.%d.1", i)
		ip := &IP{Addr: addr, Role: RoleActive, State: StateHealthy, Colo: colo, Baseline: 200, LastSeen: t0}
		for j := range 8 {
			ip.Win = append(ip.Win, Obs{OK: true, RTT: float64(200 + i*100), Colo: colo, T: t0.Add(time.Duration(j-8) * time.Second)})
		}
		d.pool.ips[addr] = ip
	}
	inc := d.pool.ips["198.18.2.1"]
	sb := &IP{Addr: "198.18.3.1", Role: RoleStandby, Colo: "KIX", LastSeen: t0}
	for j := range 8 {
		sb.Win = append(sb.Win, Obs{OK: true, RTT: 150, Colo: "KIX", T: t0.Add(time.Duration(j-8) * time.Second)})
	}
	d.pool.ips[sb.Addr] = sb
	return d, inc, sb
}

func drainJobs(d *daemon) []job {
	var out []job
	for {
		select {
		case j := <-d.jobs:
			out = append(out, j)
		default:
			return out
		}
	}
}
