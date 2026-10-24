package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
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

func TestBlockKeyCanonical(t *testing.T) {
	if got := BlockKey("172.64.52.137"); got != "172.64.52" {
		t.Fatalf("v4 key = %q", got)
	}
	if got := BlockKey("2a06:98c1:3109:b980:20cd:5db4:5d60:c502"); got != "2a06:98c1:3109::" {
		t.Fatalf("v6 key = %q", got)
	}
}

func TestLoadMergesLegacyPrefixKeys(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// v1 sampler wrote four-part keys, v1 recorder wrote three-part keys.
	snap := `{"saved_at":"2026-09-27T00:00:00Z","ips":{},"prefixes":{
		"172.64.52.0":{"prefix":"172.64.52.0","probes":3,"ok":3,"colo_ok":3,"rtt_sum":600,"rtt_count":3,"last_probe":"2026-09-27T00:00:00Z"},
		"172.64.52":{"prefix":"172.64.52","probes":2,"ok":1,"colo_ok":1,"rtt_sum":250,"rtt_count":1,"last_probe":"2026-10-10T00:00:00Z"}}}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(snap), 0o644); err != nil {
		t.Fatal(err)
	}
	d := testDaemon(t)
	if err := st.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	if len(d.scan.stats) != 1 {
		t.Fatalf("expected 1 merged prefix, got %d", len(d.scan.stats))
	}
	m := d.scan.stats["172.64.52"]
	if m == nil || m.Probes != 5 || m.OK != 4 || m.ColoOK != 4 || m.RTTCount != 4 {
		t.Fatalf("merged stat wrong: %+v", m)
	}
	if got := m.LastProbe.Format("2006-01-02"); got != "2026-10-10" {
		t.Fatalf("merged LastProbe = %s, want newest", got)
	}
}

func TestLoadMigratesQuarantine(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenStore(dir)
	defer st.Close()
	snap := `{"ips":{"1.2.3.4":{"addr":"1.2.3.4","role":"quarantine","quar_until":"2030-01-01T00:00:00Z",
		"quar_level":7,"first_seen":"2026-01-01T00:00:00Z","last_seen":"2026-01-01T00:00:00Z"}}}`
	os.WriteFile(filepath.Join(dir, "state.json"), []byte(snap), 0o644)
	d := testDaemon(t)
	before := time.Now()
	if err := st.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	ip := d.pool.get("1.2.3.4")
	if ip == nil || ip.Role != RoleCandidate {
		t.Fatalf("quarantine should migrate to candidate, got %+v", ip)
	}
	if ip.CooldownUntil.After(before.Add(d.cfg.Cooldown.Duration + time.Minute)) {
		t.Fatalf("v1 12h penalty must collapse to one bounded cooldown, got %v", ip.CooldownUntil)
	}
	if !ip.NextProbe.Equal(ip.CooldownUntil) {
		t.Fatalf("NextProbe should be the cooldown end")
	}
}

func TestPrefixGoodIsRelative(t *testing.T) {
	st := &PrefixStat{Probes: 4, OK: 4, ColoOK: 4, RTTSum: 1000, RTTCount: 4} // avg 250ms
	if !st.good(0) {
		t.Fatal("no active reference must not veto a working prefix (cold start)")
	}
	if !st.good(200) {
		t.Fatal("250ms avg within 1.3x of a 200ms reference should be good")
	}
	if st.good(150) {
		t.Fatal("250ms vs 195ms limit must fail")
	}
}

func TestPickUsesCanonicalStats(t *testing.T) {
	cfg := defaultConfig()
	cfg.ScanV6Ratio = 0
	s := NewScanner(cfg, func(string) bool { return false }, func() float64 { return 200 })
	s.rnd = rand.New(rand.NewSource(1))
	// pick() ages stats against the wall clock, so a fixed t0 goes stale.
	now := time.Now()
	// Record two good probes on 108.162.198.93 -> its /24 becomes "good".
	s.Record("108.162.198.93", okProbe(150, "NRT"), now)
	s.Record("108.162.198.93", okProbe(160, "NRT"), now)
	// Make every other prefix bad so roll<0.6 must land on the good one.
	for _, base := range s.prefixes {
		if s.statV4(base) == nil {
			s.stats[blockKeyV4([4]byte{byte(base >> 24), byte(base >> 16), byte(base >> 8), 0})] =
				&PrefixStat{Probes: 5, OK: 0, LastProbe: now}
		}
	}
	hit := false
	for range 50 {
		if got := s.pick(); BlockKey(got) == "108.162.198" {
			hit = true
			break
		}
	}
	if !hit {
		t.Fatal("sampler never selected the only good prefix: canonical key lookup broken")
	}
}

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

func TestRelayFeedbackBoundaries(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateDegraded, Colo: "NRT",
		ConsecOK: 0, NextProbe: t0.Add(time.Hour)}
	d.pool.ips[ip.Addr] = ip
	// TCP dial success: EMA only, never HTTP health recovery
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{OK: true, Totalms: 50}})
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{OK: true, Totalms: 60}})
	if ip.State != StateDegraded || ip.ConsecOK != 0 {
		t.Fatalf("relay TCP success must not restore HTTP health: state=%s consecOK=%d", ip.State, ip.ConsecOK)
	}
	// TCP dial failure: immediate faster verification
	far := time.Now().Add(time.Hour)
	ip.NextProbe = far
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{Err: "refused"}})
	if !ip.NextProbe.Before(far) {
		t.Fatalf("relay failure should pull NextProbe to now, got %v", ip.NextProbe)
	}
	if d.relayStat(ip.Addr).Fails != 1 {
		t.Fatal("relay failure should count in relay stats")
	}
}

func TestStandbyPerIPSchedule(t *testing.T) {
	d := testDaemon(t)
	d.cfg.ActiveCount = 0 // isolate from promotion/scan side effects
	due := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "NRT", NextProbe: t0.Add(-time.Minute)}
	later := &IP{Addr: "2.2.2.3", Role: RoleStandby, Colo: "NRT", NextProbe: t0.Add(time.Hour)}
	d.pool.ips[due.Addr], d.pool.ips[later.Addr] = due, later
	d.schedule(t0)
	var got []job
	for {
		select {
		case j := <-d.jobs:
			got = append(got, j)
		default:
			goto drained
		}
	}
drained:
	standbyJobs := 0
	for _, j := range got {
		if j.kind == jStandby {
			standbyJobs++
			if j.ip != due.Addr {
				t.Fatalf("due standby expected, got %s", j.ip)
			}
		}
	}
	if standbyJobs != 1 {
		t.Fatalf("expected exactly 1 standby job (the due one), got %d", standbyJobs)
	}
	if !due.NextProbe.After(t0) {
		t.Fatal("scheduled standby must get a fresh NextProbe")
	}
}

func TestCandidateDispatchNeedsPurpose(t *testing.T) {
	d := testDaemon(t)
	// full healthy pool: 3 actives + 8 standbys
	for i := range 3 {
		a := &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1), Role: RoleActive, State: StateHealthy, Colo: "NRT"}
		for range 4 {
			a.Win = append(a.Win, Obs{OK: true, RTT: 100, Colo: "NRT", T: t0})
		}
		d.pool.ips[a.Addr] = a
	}
	for i := range 8 {
		d.pool.ips[fmt.Sprintf("2.2.2.%d", i+1)] = &IP{Addr: fmt.Sprintf("2.2.2.%d", i+1),
			Role: RoleStandby, Colo: "NRT", NextProbe: t0.Add(time.Hour)}
	}
	c := &IP{Addr: "9.9.9.9", Role: RoleCandidate, NextProbe: t0.Add(-time.Minute), FirstSeen: t0}
	d.pool.ips[c.Addr] = c
	d.schedule(t0)
	for _, j := range drainJobs(d) {
		if j.kind == jValidate {
			t.Fatal("idle healthy pool must not spend candidate validation dispatches")
		}
	}
	// drop below standby target: validation resumes
	for i := 0; i < 7; i++ {
		d.pool.remove(fmt.Sprintf("2.2.2.%d", i+1))
	}
	d.schedule(t0)
	found := false
	for _, j := range drainJobs(d) {
		if j.kind == jValidate && j.ip == c.Addr {
			found = true
		}
	}
	if !found {
		t.Fatal("backup deficit should dispatch the due candidate")
	}
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

func TestStalePromoteResultRejected(t *testing.T) {
	d := testDaemon(t)
	sb := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "NRT", Promoting: true}
	d.pool.ips[sb.Addr] = sb
	d.promoting = true
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: d.pool.gen + 1},
		ok: true, med: 100, speed: SpeedResult{Mbps: 100}})
	if sb.Role != RoleStandby {
		t.Fatal("stale-generation promote result must not change roles")
	}
	if d.promoting || sb.Promoting {
		t.Fatal("stale result must still clear promoting flags")
	}
}

func TestPKPassContract(t *testing.T) {
	cfg := defaultConfig()
	mk := func(chall, inc []float64, co, io_ int) *pkRun {
		return &pkRun{challRTT: chall, incRTT: inc, challOK: co, incOK: io_}
	}
	eight := func(v float64) []float64 {
		r := make([]float64, 8)
		for i := range r {
			r[i] = v
		}
		return r
	}
	if ok, _ := mk(eight(200), eight(300), 8, 8).pass(cfg); !ok {
		t.Fatal("200 vs 300 should pass (100ms > 30ms, 33% > 20%)")
	}
	if ok, _ := mk(eight(280), eight(300), 8, 8).pass(cfg); ok {
		t.Fatal("280 vs 300: 20ms improvement must fail the 30ms rule")
	}
	if ok, _ := mk(eight(250), eight(300), 8, 8).pass(cfg); ok {
		t.Fatal("250 vs 300: 17% improvement must fail the 20% rule")
	}
	badTail := append(eight(200)[:6], 500, 500)
	if ok, _ := mk(badTail, eight(300), 8, 8).pass(cfg); ok {
		t.Fatal("worse p90 must fail")
	}
	if ok, _ := mk(eight(200), eight(300), 5, 8).pass(cfg); ok {
		t.Fatal("undersampled challenger must fail")
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
	d.pool.pruneCandidates(t0)
	if d.pool.get(cool.Addr) == nil {
		t.Fatal("cooldown candidate must survive pruning (else a failed active could be rediscovered past its rest)")
	}
	if n := len(d.pool.candidates()); n > d.cfg.CandidateCap+1 {
		t.Fatalf("cap not enforced: %d", n)
	}
}

func TestCandidatePerPrefixBound(t *testing.T) {
	d := testDaemon(t)
	for _, a := range []string{"9.9.9.1", "9.9.9.2"} {
		d.pool.ips[a] = &IP{Addr: a, Role: RoleCandidate, FirstSeen: t0}
	}
	if d.candidateRoom("9.9.9.3") {
		t.Fatal("third candidate in same /24 must be rejected at admission")
	}
	if !d.candidateRoom("9.9.10.1") {
		t.Fatal("different /24 must have room")
	}
}

func TestDiscoveryFailureFeedsPrefixOnly(t *testing.T) {
	d := testDaemon(t)
	d.apply(jobResult{job: job{kind: jDiscover, ip: "9.9.9.9"}, probe: ProbeResult{Err: "timeout"}})
	if d.pool.get("9.9.9.9") != nil {
		t.Fatal("failed discovery must not create candidate work")
	}
	if d.scan.noYield != 1 {
		t.Fatal("yield-free discovery must count toward backoff")
	}
	if d.scan.stats["9.9.9"] == nil || d.scan.stats["9.9.9"].Probes != 1 {
		t.Fatal("failed discovery must still feed prefix learning")
	}
	// wrong-colo success: still no candidate, still prefix learning
	d.apply(jobResult{job: job{kind: jDiscover, ip: "9.9.9.10"}, probe: okProbe(100, "LAX")})
	if d.pool.get("9.9.9.10") != nil {
		t.Fatal("wrong-colo discovery must not create candidate work")
	}
	// useful discovery creates a candidate
	d.apply(jobResult{job: job{kind: jDiscover, ip: "9.9.9.11"}, probe: okProbe(100, "NRT")})
	if d.pool.get("9.9.9.11") == nil || d.scan.noYield != 0 {
		t.Fatal("useful discovery should create candidate and reset yield backoff")
	}
}

func TestScanIntervalAdaptive(t *testing.T) {
	d := testDaemon(t)
	// empty pool: bootstrap
	if got := d.scanInterval(); got != d.cfg.ScanBootstrap.Duration {
		t.Fatalf("bootstrap interval = %v", got)
	}
	// deficit: fast
	d.pool.ips["2.2.2.1"] = &IP{Addr: "2.2.2.1", Role: RoleStandby}
	if got := d.scanInterval(); got != d.cfg.ScanLow.Duration {
		t.Fatalf("deficit interval = %v", got)
	}
	for i := range 3 {
		d.pool.ips[fmt.Sprintf("1.1.1.%d", i+1)] = &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1), Role: RoleActive, State: StateHealthy}
	}
	for i := range 8 {
		d.pool.ips[fmt.Sprintf("2.2.2.%d", i+1)] = &IP{Addr: fmt.Sprintf("2.2.2.%d", i+1), Role: RoleStandby}
	}
	if got := d.scanInterval(); got != d.cfg.ScanStable.Duration {
		t.Fatalf("stable base = %v", got)
	}
	d.scan.noYield = 40
	if got := d.scanInterval(); got != 4*d.cfg.ScanStable.Duration {
		t.Fatalf("40 no-yield should double twice: %v", got)
	}
	d.scan.noYield = 10000
	if got := d.scanInterval(); got != d.cfg.ScanMax.Duration {
		t.Fatalf("backoff must cap at ScanMax: %v", got)
	}
}

func TestPickPKPairOpportunity(t *testing.T) {
	d := testDaemon(t)
	for i := range 3 {
		a := &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1), Role: RoleActive, State: StateHealthy, Colo: "NRT"}
		for range 4 {
			a.Win = append(a.Win, Obs{OK: true, RTT: 200, Colo: "NRT", T: t0})
		}
		d.pool.ips[a.Addr] = a
	}
	// worst active: median 300
	worst := d.pool.ips["1.1.1.3"]
	worst.Win = []Obs{{OK: true, RTT: 300, Colo: "NRT", T: t0}, {OK: true, RTT: 300, Colo: "NRT", T: t0},
		{OK: true, RTT: 300, Colo: "NRT", T: t0}, {OK: true, RTT: 300, Colo: "NRT", T: t0}}
	sb := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "SIN", LastSeen: t0}
	for range 4 {
		sb.Win = append(sb.Win, Obs{OK: true, RTT: 200, Colo: "SIN", T: t0})
	}
	d.pool.ips[sb.Addr] = sb
	gotSB, gotInc := d.pool.pickPKPair(t0)
	if gotSB != sb || gotInc != worst {
		t.Fatalf("expected (%s,%s), got (%v,%v)", sb.Addr, worst.Addr, gotSB, gotInc)
	}
	// stale standby must not challenge
	sb.LastSeen = t0.Add(-time.Hour)
	if s, _ := d.pool.pickPKPair(t0); s != nil {
		t.Fatal("stale standby must be refreshed before PK, not trusted")
	}
	// small advantage must not trigger
	sb.LastSeen = t0
	sb.Win = []Obs{{OK: true, RTT: 290, Colo: "SIN", T: t0}, {OK: true, RTT: 290, Colo: "SIN", T: t0},
		{OK: true, RTT: 290, Colo: "SIN", T: t0}, {OK: true, RTT: 290, Colo: "SIN", T: t0}}
	if s, _ := d.pool.pickPKPair(t0); s != nil {
		t.Fatal("10ms advantage must not trigger PK")
	}
}

func TestPKObserveOnlyWindow(t *testing.T) {
	d := testDaemon(t)
	d.pool.pk = &pkPersist{ObserveUntil: t0.Add(24 * time.Hour)}
	// full pool: 3 actives, incumbent among them
	for _, a := range []string{"1.1.1.2", "1.1.1.3"} {
		d.pool.ips[a] = &IP{Addr: a, Role: RoleActive, State: StateHealthy, Colo: "SIN"}
	}
	inc := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	sb := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "KIX"}
	d.pool.ips[inc.Addr], d.pool.ips[sb.Addr] = inc, sb
	mk := func() jobResult {
		return jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr, gen: d.pool.gen},
			ok: true, speed: SpeedResult{Mbps: 100}, speed2: SpeedResult{Mbps: 90}}
	}
	d.applySpeed(mk(), t0)
	if sb.Role != RoleStandby || inc.Role != RoleActive {
		t.Fatal("observe-only window must log decisions without swapping")
	}
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	r := mk()
	r.job.gen = d.pool.gen // gen unchanged by observe-only decision
	d.applySpeed(r, t0)
	if sb.Role != RoleActive || inc.Role != RoleStandby {
		t.Fatal("after warm-up a confirmed winner should swap")
	}
}

// TestRaceEventLoopVsReaders hammers the event-loop entry points and the
// relay snapshot reader concurrently; run with -race.
func TestRaceEventLoopVsReaders(t *testing.T) {
	d := testDaemon(t)
	d.cfg.ActiveCount = 0
	d.ctx = context.Background()
	for i := range 3 {
		d.pool.ips[fmt.Sprintf("1.1.1.%d", i+1)] = &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1),
			Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				d.relaySnap.Load()
			}
		}
	}()
	for i := range 200 {
		now := t0.Add(time.Duration(i) * time.Second)
		d.schedule(now)
		d.apply(jobResult{job: job{kind: jRelay, ip: "1.1.1.1"}, probe: okProbe(100, "NRT")})
		for _, j := range drainJobs(d) {
			d.apply(jobResult{job: j, probe: okProbe(100, "NRT")})
		}
	}
	close(done)
}

// TestEstablishedConnSurvivesRoleChange: a PK/fill role swap must leave an
// already-established relay TCP connection fully functional.
func TestEstablishedConnSurvivesRoleChange(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			go io.Copy(c, c) // echo until closed
		}
	}()
	_, port, _ := net.SplitHostPort(up.Addr().String())
	d := testDaemon(t)
	fmt.Sscanf(port, "%d", &d.cfg.RelayPort)
	host := "127.0.0.1"
	d.pool.ips[host] = &IP{Addr: host, Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.updateRelaySnap()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go d.handleRelayConn(c)
		}
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ping := func(tag string) {
		t.Helper()
		cli.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := cli.Write([]byte(tag)); err != nil {
			t.Fatalf("%s write: %v", tag, err)
		}
		buf := make([]byte, len(tag))
		if _, err := io.ReadFull(cli, buf); err != nil || string(buf) != tag {
			t.Fatalf("%s echo: %v %q", tag, err, buf)
		}
	}
	ping("before")
	// Role replacement: the active leaves the pool entirely (hard-fail fill).
	d.pool.remove(host)
	d.pool.ips["127.0.0.2"] = &IP{Addr: "127.0.0.2", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.updateRelaySnap()
	ping("after") // established connection must keep flowing
}

// TestLateHealthResultDoesNotPoison: a health result dispatched before a role
// change must be dropped by per-IP generation evidence.
func TestLateHealthResultDoesNotPoison(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.pool.ips[ip.Addr] = ip
	genAtDispatch := ip.Gen
	d.pool.cooldown(ip, "fixture", t0) // role change bumps the IP generation
	d.apply(jobResult{job: job{kind: jHealth, ip: ip.Addr, gen: genAtDispatch}, probe: okProbe(100, "NRT")})
	if len(ip.Win) != 0 {
		t.Fatal("stale in-flight health result must not poison a reactivated node")
	}
}

// TestPromoteFailureBacksOff: a failed mandatory promotion must not retry the
// same standby every tick; other valid backups are tried.
func TestPromoteFailureBacksOff(t *testing.T) {
	d := testDaemon(t)
	bad := &IP{Addr: "2.2.2.1", Role: RoleStandby, Colo: "NRT", LastSeen: t0}
	good := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "SIN", LastSeen: t0}
	for range 4 {
		bad.Win = append(bad.Win, Obs{OK: true, RTT: 90, Colo: "NRT", T: t0})
		good.Win = append(good.Win, Obs{OK: true, RTT: 100, Colo: "SIN", T: t0})
	}
	d.pool.ips[bad.Addr], d.pool.ips[good.Addr] = bad, good
	d.promoting = true
	d.apply(jobResult{job: job{kind: jPromote, ip: bad.Addr, gen: d.pool.gen}, err: "speed 5.0Mbps < 30.0Mbps"})
	if _, failed := d.promoteFail[bad.Addr]; !failed {
		t.Fatal("failed promotion must record a retry backoff")
	}
	// fillActive must skip the backed-off standby and pick the other one
	d.fillActive(t0)
	if !good.Promoting {
		t.Fatal("fill must try the next valid backup, not retry the failed one")
	}
}

// TestPKPendingAgeNotResetByEarlyWin: an early second win must not postpone
// the confirmation due time.
func TestPKPendingAgeNotResetByEarlyWin(t *testing.T) {
	d, inc, sb := regressionPair(t)
	mk := func() jobResult {
		r := jobResult{job: job{kind: jPK, ip: sb.Addr, peer: inc.Addr, gen: d.pool.gen}, ok: true, pk: &pkRun{}}
		for range 8 {
			r.pk.challRTT = append(r.pk.challRTT, 150)
			r.pk.incRTT = append(r.pk.incRTT, 400)
		}
		r.pk.challOK, r.pk.incOK, r.pk.challColo, r.pk.incColo = 8, 8, "KIX", "HKG"
		return r
	}
	d.applyPK(mk(), t0)
	if d.pkPending == nil || !d.pkPending.At.Equal(t0) {
		t.Fatal("first win must arm a pending pair")
	}
	d.applyPK(mk(), t0.Add(time.Minute))
	if !d.pkPending.At.Equal(t0) {
		t.Fatal("early second win must not reset the first-win age")
	}
	d.applyPK(mk(), t0.Add(6*time.Minute))
	if d.pkPending != nil || !d.pkRunning {
		t.Fatal("second win after 5min must confirm and dispatch the bandwidth stage")
	}
	if d.pool.pk.BWUsed != 1 {
		t.Fatal("confirmed PK must consume the daily bandwidth budget")
	}
}

// TestHistoryRotationPreserves: rotation never overwrites an archive and the
// tracked size starts from the existing file.
func TestHistoryRotationPreserves(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(p, make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.histSz != 1000 {
		t.Fatalf("tracked size must initialize from existing file, got %d", st.histSz)
	}
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("rotation must move the segment aside")
	}
	// archive exists and a second rotation within the same second must not clobber it
	os.WriteFile(p, []byte("x"), 0o644)
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	n := 0
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "history.jsonl" && filepath.Ext(e.Name()) == ".jsonl" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("both archives must be preserved, found %d", n)
	}
}

// TestHistoryRotationPrunesOldest: rotation keeps only the newest histKeep
// archives. The archive files are fixtures, not production history.
func TestHistoryRotationPrunesOldest(t *testing.T) {
	dir := t.TempDir()
	var fixtures []string
	for i := range histKeep + 1 {
		f := filepath.Join(dir, fmt.Sprintf("history-2000010%d-000000.jsonl", i+1))
		if err := os.WriteFile(f, []byte("fixture\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, f)
	}
	p := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(p, []byte("fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	left, _ := filepath.Glob(filepath.Join(dir, "history-*.jsonl"))
	if len(left) != histKeep {
		t.Fatalf("want %d archives after pruning, found %d", histKeep, len(left))
	}
	for i, f := range fixtures {
		_, err := os.Stat(f)
		if pruned := os.IsNotExist(err); pruned != (i < 2) {
			t.Fatalf("archive %s: pruned=%v, want only the two oldest pruned", filepath.Base(f), pruned)
		}
	}
}
