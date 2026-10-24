package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestStalePromoteResultRejected(t *testing.T) {
	d := testDaemon(t)
	sb := &IP{Addr: "2.2.2.2", Role: RoleStandby, Colo: "NRT", Promoting: true}
	d.pool.ips[sb.Addr] = sb
	d.promoting = true
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: sb.Gen + 1},
		ok: true, med: 100, speed: SpeedResult{Mbps: 100}})
	if sb.Role != RoleStandby {
		t.Fatal("stale-generation promote result must not change roles")
	}
	if d.promoting || sb.Promoting {
		t.Fatal("stale result must still clear promoting flags")
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
	d.apply(jobResult{job: job{kind: jPromote, ip: bad.Addr, gen: bad.Gen}, err: "speed 5.0Mbps < 30.0Mbps"})
	if _, failed := d.promoteFail[bad.Addr]; !failed {
		t.Fatal("failed promotion must record a retry backoff")
	}
	// fillActive must skip the backed-off standby and pick the other one
	d.fillActive(t0)
	if !good.Promoting {
		t.Fatal("fill must try the next valid backup, not retry the failed one")
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

func TestHealthyFullPoolsStillAdmitNewCredibleImprovement(t *testing.T) {
	d, _, sb := pkPairFixture(t)
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

func TestGenerationZeroAlsoExpires(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.cooldown(inc, "fixture failure", t0) // open an active slot
	dispatched := sb.Gen
	if dispatched != 0 {
		t.Fatal("fixture: the standby must start at generation zero")
	}
	d.pool.promoteActive(sb, 150, t0)
	d.pool.demoteToStandby(sb, t0) // the standby changed role and came back
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: dispatched}, ok: true, med: 150,
		speed: SpeedResult{Mbps: 100}, probe: okProbe(150, "KIX")})
	if sb.Role != RoleStandby {
		t.Fatal("a job from generation zero must be rejected after the standby's generation changes")
	}
}

func TestRejectedDiscoveryDoesNotResetUsefulYield(t *testing.T) {
	d := testDaemon(t)
	d.cfg.CandidateCap = 1
	d.pool.ensure("198.18.0.1", t0)
	d.scan.noYield = 19
	d.apply(jobResult{job: job{kind: jDiscover, ip: "198.18.1.1"}, probe: okProbe(150, "NRT")})
	if d.scan.noYield < 20 {
		t.Fatal("an Asian discovery rejected by capacity has not produced useful candidate work")
	}
}

func TestCountsPhysicalPromotionProbes(t *testing.T) {
	var traces atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("CF-RAY", "fixture-NRT")
		if r.URL.Path == "/down" {
			w.Write(make([]byte, 1024))
			return
		}
		traces.Add(1)
		fmt.Fprint(w, "colo=NRT\n")
	}))
	t.Cleanup(srv.Close)
	d := testDaemon(t)
	d.cfg.ActiveCount, d.cfg.PromoteMinMbps = 1, 0
	d.cfg.TraceURL, d.cfg.DownURL = srv.URL, srv.URL+"/down"
	d.cfg.PromoteInterval = Duration{}
	sb := &IP{Addr: "127.0.0.1", Role: RoleStandby, Colo: "NRT", LastSeen: time.Now()}
	d.pool.ips[sb.Addr] = sb
	r := d.execPromote(context.Background(), job{kind: jPromote, ip: sb.Addr, limit: 1000, gen: sb.Gen})
	if !r.ok {
		t.Fatalf("fixture promotion failed: %s", r.err)
	}
	d.apply(r)
	count := 0
	for kind, n := range d.dayCounts {
		if kind != "relay" {
			count += n
		}
	}
	if int64(count) < traces.Load() {
		t.Fatalf("reported %d probes but fixture received %d physical trace requests", count, traces.Load())
	}
}

// TestPromotionSurvivesUnrelatedCandidateChurn: candidates leaving the pool
// must not invalidate the promotion test of an unrelated standby.
func TestPromotionSurvivesUnrelatedCandidateChurn(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.cooldown(inc, "fixture failure", t0) // open an active slot
	dispatched := sb.Gen
	d.pool.ensure("198.20.0.1", t0)
	d.pool.remove("198.20.0.1")
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: dispatched}, ok: true, med: 150,
		speed: SpeedResult{Mbps: 100}, probe: okProbe(150, "KIX")})
	if sb.Role != RoleActive {
		t.Fatal("a promotion result must stay valid while its standby keeps its role")
	}
}

// TestFillRelaxesColoCapOnlyWithoutDiverseStandby: with only same-colo
// backups, fill exceeds the per-colo cap instead of staying short; a backup
// within the cap still wins, and the per-prefix cap never relaxes.
func TestFillRelaxesColoCapOnlyWithoutDiverseStandby(t *testing.T) {
	setup := func(standbys ...*IP) *daemon {
		d := testDaemon(t)
		d.ctx = cancelledCtx()
		for _, a := range []*IP{sinActive("198.18.0.1", 700), sinActive("198.18.1.1", 700)} {
			d.pool.ips[a.Addr] = a
		}
		for _, sb := range standbys {
			d.pool.ips[sb.Addr] = sb
		}
		return d
	}
	sin := freshStandby("198.18.2.1", "SIN", 270)
	setup(sin).fillActive(t0)
	if !sin.Promoting {
		t.Fatal("with only same-colo backups, fill must exceed the per-colo cap")
	}
	sin, nrt := freshStandby("198.18.2.1", "SIN", 270), freshStandby("198.18.3.1", "NRT", 300)
	setup(sin, nrt).fillActive(t0)
	if sin.Promoting || !nrt.Promoting {
		t.Fatal("a backup within the per-colo cap must be preferred over a relaxed one")
	}
	samePrefix := freshStandby("198.18.0.2", "SIN", 270)
	setup(samePrefix).fillActive(t0)
	if samePrefix.Promoting {
		t.Fatal("the per-prefix cap must hold even when the per-colo cap relaxes")
	}
}

// TestRelaxedFillCommitsSameColoActive: a promotion dispatched under the
// relaxed per-colo cap commits under the same cap; a strict one does not.
func TestRelaxedFillCommitsSameColoActive(t *testing.T) {
	for _, relax := range []bool{true, false} {
		d := testDaemon(t)
		for _, a := range []*IP{sinActive("198.18.0.1", 700), sinActive("198.18.1.1", 700)} {
			d.pool.ips[a.Addr] = a
		}
		sb := freshStandby("198.18.2.1", "SIN", 270)
		d.pool.ips[sb.Addr] = sb
		d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: sb.Gen, relax: relax}, ok: true, med: 270,
			speed: SpeedResult{Mbps: 100}, probe: okProbe(270, "SIN")})
		if got := sb.Role == RoleActive; got != relax {
			t.Fatalf("relax=%v: promoted=%v", relax, got)
		}
	}
}

// TestFillBackoffDoublesPerStandby: a standby that keeps failing its
// promotion test is retried after 10 minutes, then 20, 40 and so on up to a
// day, so one bad backup cannot spend a download every 10 minutes.
func TestFillBackoffDoublesPerStandby(t *testing.T) {
	d := testDaemon(t)
	d.ctx = cancelledCtx()
	for _, a := range []*IP{sinActive("198.18.0.1", 700), sinActive("198.18.1.1", 700)} {
		d.pool.ips[a.Addr] = a
	}
	sb := freshStandby("198.18.2.1", "SIN", 270)
	d.pool.ips[sb.Addr] = sb
	attempts := 0
	for now := t0; now.Before(t0.Add(24 * time.Hour)); now = now.Add(time.Minute) {
		sb.LastSeen = now
		if d.fillActive(now) && d.promoting {
			attempts++
			d.promoting, sb.Promoting = false, false
			d.backOffPromotion(sb.Addr, now)
		}
	}
	if attempts != 8 {
		t.Fatalf("want 8 attempts in a day (at 0, 10, 30, 70, 150, 310, 630, 1270 min), got %d", attempts)
	}
}

// TestDownloadsKeepSpeedGap: no long job that may download starts before the
// gap after the previous download has passed; with no active at all, fill
// does not wait.
func TestDownloadsKeepSpeedGap(t *testing.T) {
	setup := func(actives int) (*daemon, *IP) {
		d := testDaemon(t)
		d.ctx = cancelledCtx()
		for i := range actives {
			a := sinActive(fmt.Sprintf("198.18.%d.1", i), 700)
			d.pool.ips[a.Addr] = a
		}
		sb := freshStandby("198.18.9.1", "NRT", 270)
		d.pool.ips[sb.Addr] = sb
		d.pool.speedNext = t0.Add(5 * time.Minute)
		return d, sb
	}
	d, sb := setup(2)
	d.schedule(t0)
	if sb.Promoting || d.pkRunning {
		t.Fatal("long jobs must wait for the download gap")
	}
	later := t0.Add(6 * time.Minute)
	sb.LastSeen = later
	d.schedule(later)
	if !sb.Promoting {
		t.Fatal("fill must start once the gap has passed")
	}
	d, sb = setup(0)
	d.schedule(t0)
	if !sb.Promoting {
		t.Fatal("with no active at all, fill must not wait for the gap")
	}
}

// TestPromotionBackoffCapsAtADay: the doubling backoff stays in the future and
// never exceeds a day, however long a standby keeps failing.
func TestPromotionBackoffCapsAtADay(t *testing.T) {
	d := testDaemon(t)
	now := t0
	for i := range 80 {
		d.backOffPromotion("198.18.2.1", now)
		b := d.promoteFail["198.18.2.1"]
		if wait := b.until.Sub(now); wait <= 0 || wait > 24*time.Hour {
			t.Fatalf("failure %d: backoff %v, want (0, 24h]", i+1, wait)
		}
		now = b.until
	}
}

// TestRateLimitPauseHoldsWithoutActives: the HTTP 429 pause applies even with
// no active (only the gap is waived then), and a 429 does not count against
// the standby whose test hit it.
func TestRateLimitPauseHoldsWithoutActives(t *testing.T) {
	d := testDaemon(t)
	d.ctx = cancelledCtx()
	sb := freshStandby("198.18.9.1", "NRT", 270)
	d.pool.ips[sb.Addr] = sb
	d.noteDownload(t0, SpeedResult{Err: "status 429", Status: 429})
	d.schedule(t0.Add(time.Minute))
	if sb.Promoting {
		t.Fatal("no test may start during the HTTP 429 pause, even with no active")
	}
	later := t0.Add(2 * time.Hour)
	sb.LastSeen = later
	d.schedule(later)
	if !sb.Promoting {
		t.Fatal("fill must resume once the pause ends")
	}
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: sb.Gen}, err: "speed 0.0Mbps < 30.0Mbps (status 429)",
		speed: SpeedResult{Err: "status 429", Status: 429}})
	if _, backedOff := d.promoteFail[sb.Addr]; backedOff {
		t.Fatal("an endpoint-side 429 must not back the standby off")
	}
}
