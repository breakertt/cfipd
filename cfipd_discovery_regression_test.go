package main

import (
	"fmt"
	"testing"
)

func TestHealthyFullPoolsStillAdmitNewCredibleImprovement(t *testing.T) {
	d, _, sb := regressionPair(t)
	for i := range sb.Win {
		sb.Win[i].RTT = 450
	}
	for i := 1; i < d.cfg.StandbyMax; i++ {
		addr := fmt.Sprintf("198.19.%d.1", i)
		d.pool.ips[addr] = &IP{Addr: addr, Role: RoleStandby, Colo: "NRT", LastSeen: t0,
			Win: []Obs{{OK: true, RTT: 450, Colo: "NRT", T: t0}}}
	}
	// Discovery must judge the new sample itself. Requiring an already
	// promising Candidate first would make improvement impossible from an
	// empty validation queue whenever the healthy pools are full.
	addr := "198.20.0.1"
	d.apply(jobResult{job: job{kind: jDiscover, ip: addr}, probe: okProbe(100, "KIX")})
	if ip := d.pool.get(addr); ip == nil || ip.Role != RoleCandidate {
		t.Fatal("a new credible improvement must enter validation even when healthy pools are full and no candidate exists yet")
	}
}
