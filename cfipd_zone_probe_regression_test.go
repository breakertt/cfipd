package main

import (
	"strings"
	"testing"
	"time"
)

// restrictedProbe is what probe() returns when the candidate IP answers
// Cloudflare error 1034 for the zone probe host.
func restrictedProbe() ProbeResult {
	return ProbeResult{Hard: true, Restricted: true, Colo: "NRT", Err: "zone example.com: error 1034 (edge IP restricted)"}
}

func TestZoneProbeRestrictedCandidateIsDropped(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.9.1", Role: RoleCandidate, LastSeen: t0}
	d.pool.ips[ip.Addr] = ip
	msgs := d.pool.applyCandidate(ip, restrictedProbe(), t0)
	if d.pool.get(ip.Addr) != nil {
		t.Fatal("a candidate answering 1034 must leave the pool, not finish its validation round")
	}
	if len(msgs) != 1 || !strings.Contains(msgs[0], "candidate dropped") {
		t.Fatalf("unexpected event log: %v", msgs)
	}
}

func TestZoneProbeRestrictedStandbyIsCooledDownAtOnce(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.9.2", Role: RoleStandby, Colo: "NRT", LastSeen: t0}
	for j := range 8 {
		ip.Win = append(ip.Win, Obs{OK: true, RTT: 150, Colo: "NRT", T: t0.Add(time.Duration(j-8) * time.Second)})
	}
	d.pool.ips[ip.Addr] = ip
	d.pool.applyStandby(ip, restrictedProbe(), t0)
	if ip.Role != RoleCandidate || !ip.CooldownUntil.After(t0) {
		t.Fatalf("standby must be parked as a cooling-down candidate after one 1034 (no two-strike rule), got role=%v cooldown=%v", ip.Role, ip.CooldownUntil)
	}
}

func TestZoneProbeRestrictedMakesThePrefixBadImmediately(t *testing.T) {
	cfg := defaultConfig()
	s := NewScanner(cfg, func(string) bool { return false }, func() float64 { return 200 })
	// A /24 with a long good history (trace probes pass on every CF IP) ...
	for range 30 {
		s.Record("162.159.39.22", okProbe(170, "NRT"), t0)
	}
	// ... turns bad with the first 1034, not after dozens of fresh failures.
	s.Record("162.159.39.23", restrictedProbe(), t0)
	st := s.statV4(0xA29F2700)
	if st == nil || !st.bad() {
		t.Fatalf("prefix must be learned as bad right after one 1034, got %+v", st)
	}
}
