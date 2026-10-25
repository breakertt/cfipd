package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestApplyCandidateTruthfulObs(t *testing.T) {
	d := testDaemon(t)
	ip := d.pool.ensure("10.0.0.1", t0)
	t1, t2, t3 := t0, t0.Add(time.Minute), t0.Add(2*time.Minute)
	d.pool.applyCandidate(ip, okProbe(100, "NRT"), t1)
	d.pool.applyCandidate(ip, ProbeResult{Err: "timeout"}, t2)
	if len(ip.Win) != 2 || !ip.Win[0].T.Equal(t1) || ip.Win[1].OK {
		t.Fatalf("win must keep real times and failures: %+v", ip.Win)
	}
	ev := d.pool.applyCandidate(ip, okProbe(110, "NRT"), t3)
	if d.pool.get("10.0.0.1") == nil {
		t.Fatalf("one soft failure must be tolerated, round must not delete: %v", ev)
	}
	if ip.Role != RoleStandby {
		t.Fatalf("passing round should graduate to standby, got %s (events %v)", ip.Role, ev)
	}
	if !ip.Win[0].T.Equal(t1) {
		t.Fatalf("graduation must not rewrite observation times: %+v", ip.Win)
	}
}

func TestApplyCandidateFailRoundLeaves(t *testing.T) {
	d := testDaemon(t)
	ip := d.pool.ensure("10.0.0.2", t0)
	for i := range 3 {
		d.pool.applyCandidate(ip, ProbeResult{Err: "timeout"}, t0.Add(time.Duration(i)*time.Minute))
	}
	if d.pool.get("10.0.0.2") != nil {
		t.Fatal("failed round must end with the candidate leaving the pool, not endless resets")
	}
}

func TestHealthRecoveryRequiresImprovement(t *testing.T) {
	d := testDaemon(t)
	cfg := d.cfg
	ip := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateHealthy, Colo: "NRT", Baseline: 100}
	d.pool.ips[ip.Addr] = ip
	// establish baseline window
	for i := range 6 {
		d.pool.applyHealth(ip, okProbe(100, "NRT"), t0.Add(time.Duration(i)*10*time.Second))
	}
	ip.ConsecOK = 5 // simulate long healthy run
	// two spikes degrade
	d.pool.applyHealth(ip, okProbe(300, "NRT"), t0.Add(70*time.Second))
	d.pool.applyHealth(ip, okProbe(300, "NRT"), t0.Add(80*time.Second))
	if ip.State != StateDegraded {
		t.Fatalf("expected degraded after 2 spikes, baseline %.0f", ip.Baseline)
	}
	if ip.ConsecOK != 0 {
		t.Fatalf("degrade must reset recovery counter, got %d", ip.ConsecOK)
	}
	// successful but still slow: must NOT recover
	for i := range cfg.RecoverOKs + 2 {
		d.pool.applyHealth(ip, okProbe(300, "NRT"), t0.Add(time.Duration(90+i*10)*time.Second))
	}
	if ip.State == StateHealthy {
		t.Fatal("slow-but-OK probes must not recover a spike-degraded active")
	}
	// back near baseline recovers
	for i := range cfg.RecoverOKs {
		d.pool.applyHealth(ip, okProbe(110, "NRT"), t0.Add(time.Duration(150+i*10)*time.Second))
	}
	if ip.State != StateHealthy {
		t.Fatal("genuinely recovered node should become healthy")
	}
}

func TestHealthAdaptiveInterval(t *testing.T) {
	cfg := defaultConfig()
	ip := &IP{Role: RoleActive, State: StateHealthy}
	if d := ip.healthEvery(cfg); d != 10*time.Second {
		t.Fatalf("base = %v", d)
	}
	ip.StableOK = 5
	if d := ip.healthEvery(cfg); d != 30*time.Second {
		t.Fatalf("tier1 = %v", d)
	}
	ip.StableOK = 10
	if d := ip.healthEvery(cfg); d != 60*time.Second {
		t.Fatalf("tier2 = %v", d)
	}
	ip.StableOK = 15
	if d := ip.healthEvery(cfg); d != 120*time.Second {
		t.Fatalf("tier3 = %v", d)
	}
	ip.ConsecFail = 1
	if d := ip.healthEvery(cfg); d != 2*time.Second {
		t.Fatalf("anomaly recheck = %v", d)
	}
	// jitter must never exceed the nominal tier
	p := NewPool(cfg)
	for range 100 {
		if got := p.jitterDown(ip.healthEvery(cfg)); got > 120*time.Second && ip.StableOK >= 15 && ip.ConsecFail == 0 {
			t.Fatalf("jitter exceeded maximum: %v", got)
		}
	}
}

func TestSlowActiveNotEvictedWithoutReplacement(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateDegraded,
		DegradedSince: t0.Add(-time.Hour), Colo: "NRT"}
	d.pool.ips[ip.Addr] = ip
	if r := d.pool.checkDown(ip, t0); r != "" {
		t.Fatalf("slow-but-usable active must be kept when no standby exists: %s", r)
	}
	d.pool.ips["2.2.2.2"] = &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "NRT"}
	if r := d.pool.checkDown(ip, t0); r != "" {
		t.Fatalf("unproven standby must not authorize eviction: %s", r)
	}
	// Fresh sampled backup evidence still has not passed the complete
	// promotion/PK contract, so it cannot authorize withdrawing the node.
	sb := d.pool.ips["2.2.2.2"]
	sb.LastSeen = t0
	for range 3 {
		sb.Win = append(sb.Win, Obs{OK: true, RTT: 100, Colo: "SIN", T: t0})
	}
	if r := d.pool.checkDown(ip, t0); r != "" {
		t.Fatal("a usable active must remain until the atomic qualified swap")
	}
}

func TestPKSwapAtomic(t *testing.T) {
	d := testDaemon(t)
	for i := range 3 {
		a := &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1), Role: RoleActive, State: StateHealthy, Colo: "NRT"}
		d.pool.ips[a.Addr] = a
	}
	inc := d.pool.ips["1.1.1.2"]
	inc.Win = []Obs{{OK: true, RTT: 300, Colo: "NRT", T: t0}}
	sb := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "SIN"}
	d.pool.ips[sb.Addr] = sb
	d.pool.swapPK(sb, inc, 200, t0)
	if n := len(d.pool.actives()); n != 3 {
		t.Fatalf("swap must preserve ActiveCount=3, got %d", n)
	}
	if inc.Role != RoleStandby || len(inc.Win) != 1 {
		t.Fatalf("healthy old active must become standby with observations kept: role=%s win=%d", inc.Role, len(inc.Win))
	}
	if sb.Role != RoleActive || sb.Baseline != 200 {
		t.Fatalf("challenger must be active with new baseline: role=%s baseline=%.0f", sb.Role, sb.Baseline)
	}
}

func TestCooldownSurvivesPrune(t *testing.T) {
	d := testDaemon(t)
	d.cfg.CandidateCap = 2
	cool := &IP{Addr: "9.9.9.1", Role: RoleCandidate, CooldownUntil: t0.Add(10 * time.Minute),
		NextProbe: t0.Add(10 * time.Minute), FirstSeen: t0, LastSeen: t0}
	d.pool.ips[cool.Addr] = cool
	for i := range 3 {
		d.pool.ips[fmt.Sprintf("9.9.8.%d", i+1)] = &IP{Addr: fmt.Sprintf("9.9.8.%d", i+1),
			Role: RoleCandidate, FirstSeen: t0, LastSeen: t0}
	}
	d.pool.pruneCandidates(t0, d.cfg.CandidateCap)
	if d.pool.get(cool.Addr) == nil {
		t.Fatal("cooldown candidate must survive pruning (else a failed active could be rediscovered past its rest)")
	}
	if n := len(d.pool.candidates()); n > d.cfg.CandidateCap+1 {
		t.Fatalf("cap not enforced: %d", n)
	}
}

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

func TestRecoveryNeedsConsecutiveStableSamples(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.0.1", Role: RoleActive, State: StateDegraded, Colo: "NRT", Baseline: 100}
	d.pool.ips[ip.Addr] = ip
	for i := range 4 {
		d.pool.applyHealth(ip, okProbe(300, "NRT"), t0.Add(time.Duration(i)*time.Second))
	}
	d.pool.applyHealth(ip, okProbe(100, "NRT"), t0.Add(5*time.Second))
	if ip.State == StateHealthy {
		t.Fatal("one stable sample must not recover after several successful-but-still-slow samples")
	}
}

func TestSlowAvailableNodeNeedsQualifiedReplacement(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	inc.State, inc.DegradedSince = StateDegraded, t0.Add(-time.Hour)
	if reason := d.pool.checkDown(inc, t0); reason != "" {
		t.Fatalf("fresh trace samples alone must not bypass PK/promotion validation or observation-only period: %s", reason)
	}
	sb.Win, sb.Speed = nil, 0
	if reason := d.pool.checkDown(inc, t0); reason != "" {
		t.Fatalf("a slow available node must keep serving until replacement qualifies: %s", reason)
	}
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
