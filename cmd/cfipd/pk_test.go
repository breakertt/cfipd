package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

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
	gotSB, gotInc := d.pool.pickPKPair(t0, nil)
	if gotSB != sb || gotInc != worst {
		t.Fatalf("expected (%s,%s), got (%v,%v)", sb.Addr, worst.Addr, gotSB, gotInc)
	}
	// stale standby must not challenge
	sb.LastSeen = t0.Add(-time.Hour)
	if s, _ := d.pool.pickPKPair(t0, nil); s != nil {
		t.Fatal("stale standby must be refreshed before PK, not trusted")
	}
	// small advantage must not trigger
	sb.LastSeen = t0
	sb.Win = []Obs{{OK: true, RTT: 290, Colo: "SIN", T: t0}, {OK: true, RTT: 290, Colo: "SIN", T: t0},
		{OK: true, RTT: 290, Colo: "SIN", T: t0}, {OK: true, RTT: 290, Colo: "SIN", T: t0}}
	if s, _ := d.pool.pickPKPair(t0, nil); s != nil {
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
		return jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr, gen: sb.Gen, peerGen: inc.Gen},
			ok: true, speed: SpeedResult{Mbps: 100}, speed2: SpeedResult{Mbps: 90}}
	}
	d.pkLastRun = &pkRun{challOK: 8, incOK: 8, challRTT: []float64{200}, incRTT: []float64{260}} // fixture paired medians
	d.applySpeed(mk(), t0)
	if sb.Role != RoleStandby || inc.Role != RoleActive {
		t.Fatal("observe-only window must log decisions without swapping")
	}
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	r := mk()
	d.applySpeed(r, t0)
	if sb.Role != RoleActive || inc.Role != RoleStandby {
		t.Fatal("after warm-up a confirmed winner should swap")
	}
}

// TestPKPendingAgeNotResetByEarlyWin: an early second win must not postpone
// the confirmation due time.
func TestPKPendingAgeNotResetByEarlyWin(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	mk := func() jobResult {
		r := jobResult{job: job{kind: jPK, ip: sb.Addr, peer: inc.Addr, gen: sb.Gen, peerGen: inc.Gen}, ok: true, pk: &pkRun{}}
		for range 8 {
			r.pk.challRTT = append(r.pk.challRTT, 300) // a win, but not urgent: it needs confirmation
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

func TestPKConfirmationKeepsOriginalPairWhenRankingChanges(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pkPending = &pkPending{Challenger: sb.Addr, Incumbent: inc.Addr, At: t0}
	d.pkLastEval = t0
	better := &IP{Addr: "198.18.4.1", Role: RoleStandby, Colo: "KIX", LastSeen: t0, Win: append([]Obs(nil), sb.Win...)}
	for i := range better.Win {
		better.Win[i].RTT = 100
	}
	d.pool.ips[better.Addr] = better
	// A different active is now worst, but the original pair still qualifies.
	for i := range d.pool.ips["198.18.1.1"].Win {
		d.pool.ips["198.18.1.1"].Win[i].RTT = 500
	}
	gotSB, gotInc := d.nextPKPair(t0.Add(6 * time.Minute))
	if gotSB != sb || gotInc != inc {
		t.Fatalf("confirmation must repeat its original pair, got (%v, %v)", gotSB, gotInc)
	}
}

func TestInvalidPKPendingPairDoesNotBypassElectiveLimit(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	for i := range sb.Win {
		sb.Win[i].RTT = 300 // a credible but not urgent challenger (400 ms incumbent)
	}
	d.pkPending = &pkPending{Challenger: "198.18.9.1", Incumbent: inc.Addr, At: t0}
	d.pkLastEval = t0
	if challenger, _ := d.nextPKPair(t0.Add(6 * time.Minute)); challenger != nil {
		t.Fatal("expired confirmation must not authorize a new pair before 30min")
	}
	if d.pkPending != nil {
		t.Fatal("invalid pending pair must be cleared")
	}
	// Once the elective deadline arrives, fresh evidence can start a new pair.
	sb.LastSeen = t0.Add(30 * time.Minute)
	if challenger, incumbent := d.nextPKPair(t0.Add(30 * time.Minute)); challenger != sb || incumbent != inc {
		t.Fatal("new pairs must remain possible at the ordinary elective deadline")
	}
}

func TestPKCanImproveDegradedButAvailableIncumbent(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	inc.State = StateDegraded
	gotSB, gotInc := d.pool.pickPKPair(t0, nil)
	if gotSB != sb || gotInc != inc {
		t.Fatalf("PK should target the slow usable incumbent, got challenger=%v incumbent=%v", gotSB, gotInc)
	}
}

func TestPKCannotQualifyWithFailedHTTPChecks(t *testing.T) {
	// Not urgent (140 ms incumbent), so the ordinary availability rule applies.
	r := &pkRun{challOK: 6, incOK: 8, challRTT: []float64{100, 100, 100, 100, 100, 100}, incRTT: []float64{140, 140, 140, 140, 140, 140, 140, 140}}
	if ok, _ := r.pass(defaultConfig()); ok {
		t.Fatal("a final-qualified challenger cannot silently accept 2 failed/wrong-colo checks out of 8")
	}
}

func TestMissingBandwidthReferenceIsNotAWin(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 100, Bytes: 1024, Colo: "KIX"}, speed2: SpeedResult{Err: "status 403"}}, t0)
	if sb.Role != RoleStandby || inc.Role != RoleActive {
		t.Fatal("an unavailable incumbent bandwidth reference cannot authorize an elective swap")
	}
}

// TestPKRunsWhenFillCannotComplete: PK must still improve a short pool when
// fill has no backup it can try.
func TestPKRunsWhenFillCannotComplete(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	d.pool.cooldown(d.pool.ips["198.18.0.1"], "fixture failure", t0)        // 2 actives left
	d.promoteFail[sb.Addr] = backoff{until: t0.Add(10 * time.Minute), n: 1} // the only backup just failed promotion
	d.schedule(t0)
	if !d.pkRunning || d.promoting {
		t.Fatal("PK must run when fill cannot complete the pool")
	}
}

// TestPKSwapCommitsWhenPoolShort: a swap is one-for-one, so a short pool must
// not block it.
func TestPKSwapCommitsWhenPoolShort(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.pool.cooldown(d.pool.ips["198.18.0.1"], "fixture failure", t0)
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 100, Colo: "KIX"}, speed2: SpeedResult{Mbps: 90, Colo: "HKG"}}, t0)
	if sb.Role != RoleActive || inc.Role != RoleStandby {
		t.Fatal("a qualified one-for-one swap must commit in a short pool")
	}
}

// TestPKSameColoSwapAtColoCap: replacing an active with a backup of the same
// colo leaves colo counts unchanged, so it must pass a pool at the cap.
func TestPKSameColoSwapAtColoCap(t *testing.T) {
	d := testDaemon(t)
	for _, a := range []*IP{sinActive("198.18.0.1", 700), sinActive("198.18.1.1", 700), sinActive("198.18.2.1", 700)} {
		d.pool.ips[a.Addr] = a
	}
	sb := freshStandby("198.18.3.1", "SIN", 270)
	d.pool.ips[sb.Addr] = sb
	if challenger, _ := d.pool.pickPKPair(t0, nil); challenger != sb {
		t.Fatal("a same-colo challenger must be eligible when the pool is at the per-colo cap")
	}
}

// TestPKStaleWhenIncumbentChanges: a bandwidth result is void once the
// incumbent's generation moves, even if it holds the same role again.
func TestPKStaleWhenIncumbentChanges(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	r := jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr, gen: sb.Gen, peerGen: inc.Gen}, ok: true,
		speed: SpeedResult{Mbps: 100, Colo: "KIX"}, speed2: SpeedResult{Mbps: 90, Colo: "HKG"}}
	d.pool.demoteToStandby(inc, t0)
	d.pool.promoteActive(inc, 400, t0)
	d.pkRunning = true
	d.apply(r)
	if sb.Role != RoleStandby || inc.Role != RoleActive || d.pkRunning {
		t.Fatal("a stale incumbent generation must void the swap and clear the PK flag")
	}
}

// TestPKBandwidthLossBacksOffAndRecordsSpeed: a challenger that loses the
// bandwidth stage keeps its measured speed and the pair backs off for a day,
// so the next elective slot does not spend the daily budget on it again.
func TestPKBandwidthLossBacksOffAndRecordsSpeed(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.pkRunning = true
	d.apply(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr, gen: sb.Gen, peerGen: inc.Gen}, ok: true,
		speed: SpeedResult{Mbps: 5, Colo: "KIX"}, speed2: SpeedResult{Mbps: 90, Colo: "HKG"}})
	if sb.Speed != 5 || inc.Speed != 90 {
		t.Fatalf("both measured speeds must be recorded, got %v and %v", sb.Speed, inc.Speed)
	}
	if until := d.pkFailUntil[sb.Addr+">"+inc.Addr]; time.Until(until) < 23*time.Hour {
		t.Fatalf("a bandwidth loss must back the pair off for a day, until=%v", until)
	}
	d.pool.speedNext = time.Time{} // fixture: the download gap has passed
	later := t0.Add(31 * time.Minute)
	sb.LastSeen = later
	d.pkLastEval = t0
	d.evalPK(later)
	if d.pkRunning {
		t.Fatal("the losing pair must not be compared again at the next elective slot")
	}
}

// TestLossyIncumbentLosesPK: a failed incumbent probe counts as slower than
// any success, so a slow and lossy incumbent loses to a fully available
// challenger, while a faster incumbent keeps its slot despite one failure.
func TestLossyIncumbentLosesPK(t *testing.T) {
	cfg := defaultConfig()
	pass := func(chall, inc float64, incOK int) bool {
		r := &pkRun{challOK: 8, incOK: incOK}
		for range 8 {
			r.challRTT = append(r.challRTT, chall)
		}
		for range incOK {
			r.incRTT = append(r.incRTT, inc)
		}
		ok, _ := r.pass(cfg)
		return ok
	}
	if !pass(270, 723, 7) {
		t.Fatal("270ms 8/8 must beat a 723ms incumbent with 7/8")
	}
	if !pass(270, 723, 0) {
		t.Fatal("an incumbent without a successful probe must lose")
	}
	if pass(250, 200, 7) {
		t.Fatal("a faster incumbent must keep its slot despite one failure")
	}
}

// TestInconclusiveBandwidthStageBacksOffBriefly: a failed download makes the
// bandwidth stage inconclusive, which holds the pair for an hour, not a day.
func TestInconclusiveBandwidthStageBacksOffBriefly(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 100, Colo: "KIX"}, speed2: SpeedResult{Err: "status 429", Status: 429}}, t0)
	if wait := d.pkFailUntil[sb.Addr+">"+inc.Addr].Sub(t0); wait <= 0 || wait > time.Hour {
		t.Fatalf("an inconclusive bandwidth stage must hold the pair for at most an hour, got %v", wait)
	}
}

// TestPKVerdictShowsIncumbentFailures: the verdict names failed incumbent
// probes instead of printing an infinite latency.
func TestPKVerdictShowsIncumbentFailures(t *testing.T) {
	r := &pkRun{challOK: 8, incOK: 3}
	for range 8 {
		r.challRTT = append(r.challRTT, 270)
	}
	for range 3 {
		r.incRTT = append(r.incRTT, 723)
	}
	ok, why := r.pass(defaultConfig())
	if !ok || strings.Contains(why, "Inf") || !strings.Contains(why, "3/8") {
		t.Fatalf("ok=%v why=%q", ok, why)
	}
}

// TestPKVerdictShowsFailedIncumbent: an incumbent that failed half of its
// paired probes has no median; the verdict says "failed", not "+Infms".
func TestPKVerdictShowsFailedIncumbent(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.pkLastRun = &pkRun{challOK: 8, incOK: 4, challRTT: []float64{200}, incRTT: []float64{260, 260, 260, 260}}
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 20, Colo: "KIX"}, speed2: SpeedResult{Mbps: 500, Colo: "HKG"}}, t0)
	if strings.Contains(d.pkLastVerdict, "Inf") || !strings.Contains(d.pkLastVerdict, "failed") {
		t.Fatalf("verdict %q", d.pkLastVerdict)
	}
}

// TestScoreClampsWeight: a bandwidth_weight outside [0, 1] that a lenient
// load let through must not invert the ranking.
func TestScoreClampsWeight(t *testing.T) {
	if score(2, 200, 50) > score(2, 100, 50) || score(-1, 100, 50) != score(0, 100, 50) {
		t.Fatal("score must clamp the weight to [0, 1]")
	}
}

// TestPKBandwidthStageUsesScore: the bandwidth stage swaps when the
// challenger's score (rtt^-(1-w) * Mbps^w) on the fresh paired medians beats
// the incumbent's. The promotion floor still applies, except on a link where
// the incumbent itself measures under it. Speeds and medians are fixtures.
func TestPKBandwidthStageUsesScore(t *testing.T) {
	for _, tc := range []struct {
		w          float64
		chall, inc float64
		swap       bool
	}{
		{0.4, 70, 100, true},  // 1.3^0.6 * 0.7^0.4 = 1.015
		{0.4, 60, 100, false}, // 1.3^0.6 * 0.6^0.4 = 0.954
		{0.4, 25, 40, false},  // challenger under the 30 Mbps floor
		{0.4, 25, 18, true},   // slow link: both under the floor
		{0.4, 14, 18, true},   // 1.3^0.6 * (14/18)^0.4 = 1.058
		{0.4, 11, 18, false},  // 1.3^0.6 * (11/18)^0.4 = 0.961
		{0, 31, 100, true},    // latency only: the floor is the whole bandwidth rule
		{1, 99, 100, false},   // bandwidth only
		{1, 101, 100, true},
	} {
		d, inc, sb := pkPairFixture(t)
		d.cfg.BandwidthWeight = tc.w
		d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
		d.pkLastRun = &pkRun{challOK: 8, incOK: 8, challColo: "KIX", incColo: "HKG"}
		for range 8 {
			d.pkLastRun.challRTT = append(d.pkLastRun.challRTT, 200)
			d.pkLastRun.incRTT = append(d.pkLastRun.incRTT, 260)
		}
		d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
			speed: SpeedResult{Mbps: tc.chall, Colo: "KIX"}, speed2: SpeedResult{Mbps: tc.inc, Colo: "HKG"}}, t0)
		if swapped := sb.Role == RoleActive; swapped != tc.swap {
			t.Errorf("w=%v: 200ms/%v Mbps vs 260ms/%v Mbps: swapped=%v, want %v", tc.w, tc.chall, tc.inc, swapped, tc.swap)
		}
	}
}

// An urgent pair (incumbent at least urgentRatio times slower) starts a PK
// after urgentPKEvery instead of waiting for the 30-minute elective slot.
// Medians are fixtures.
func TestUrgentPKRunsSooner(t *testing.T) {
	for _, tc := range []struct {
		rtt   float64 // challenger window median; the incumbent is at 400 ms
		start bool
	}{{150, true}, {300, false}} {
		d, _, sb := pkPairFixture(t)
		d.ctx = cancelledCtx()
		for i := range sb.Win {
			sb.Win[i].RTT = tc.rtt
		}
		d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
		d.pkLastEval = t0.Add(-6 * time.Minute)
		d.evalPK(t0)
		if d.pkRunning != tc.start {
			t.Errorf("challenger %v ms vs 400 ms, 6 minutes after the last PK: started=%v, want %v", tc.rtt, d.pkRunning, tc.start)
		}
	}
}

// A pair on hold after a lost round does not block the next challenger.
func TestPKSkipsHeldPair(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	other := freshStandby("198.18.4.1", "KIX", 160)
	d.pool.ips[other.Addr] = other
	d.pkFailUntil[sb.Addr+">"+inc.Addr] = t0.Add(time.Hour)
	d.pkFailUntil[other.Addr+">"+inc.Addr] = time.Time{}
	d.pkLastEval = t0.Add(-time.Hour)
	d.evalPK(t0)
	if !d.pkRunning {
		t.Fatal("a held pair must not stop the next eligible challenger")
	}
}

// An urgent round has urgentProbes probes per side and passes with one lost
// challenger probe; an ordinary round needs every probe. Probe times are
// fixtures.
func TestUrgentPKToleratesLoss(t *testing.T) {
	run := func(probes int, chall, inc float64, challOK int) bool {
		r := &pkRun{probes: probes, challOK: challOK, incOK: probes}
		for range challOK {
			r.challRTT = append(r.challRTT, chall)
		}
		for range probes {
			r.incRTT = append(r.incRTT, inc)
		}
		ok, _ := r.pass(defaultConfig())
		return ok
	}
	if !run(urgentProbes, 230, 776, urgentProbes) || !run(urgentProbes, 230, 776, urgentProbes-1) {
		t.Error("an urgent round passes with all probes or one lost")
	}
	if run(urgentProbes, 230, 776, urgentProbes-2) {
		t.Error("two lost probes of four are too many")
	}
	if run(8, 230, 300, 7) {
		t.Error("an ordinary round still needs every challenger probe")
	}
}

// The incumbent's reference is the slower of its window median and the median
// of its latest successful probes, so a fresh slowdown counts at once.
// Probe times are fixtures.
func TestUrgentPKSeesFreshSlowdown(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	for i := range inc.Win {
		inc.Win[i].RTT = 270 // eight fast samples...
	}
	for i := range 4 {
		inc.Win[len(inc.Win)-1-i].RTT = 700 // ...then four slow ones
	}
	for i := range sb.Win {
		sb.Win[i].RTT = 280
	}
	if got := pkRef(inc); got != 700 {
		t.Fatalf("reference %v, want the recent 700 ms", got)
	}
	if s, i := d.pool.pickPKPair(t0, nil); s != sb || i != inc || !urgentPair(sb.medianRTT(), pkRef(inc)) {
		t.Fatalf("a fresh 700 ms slowdown against a 280 ms standby must make an urgent pair, got %v vs %v", s, i)
	}
}

// An urgent winner goes straight to the bandwidth stage, without the
// confirmation round.
func TestUrgentPKSkipsConfirmation(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	r := &pkRun{probes: urgentProbes, challOK: urgentProbes, incOK: urgentProbes}
	for range urgentProbes {
		r.challRTT = append(r.challRTT, 150)
		r.incRTT = append(r.incRTT, 400)
	}
	d.applyPK(jobResult{job: job{kind: jPK, ip: sb.Addr, peer: inc.Addr}, pk: r}, t0)
	if !d.pkRunning || d.pkPending != nil {
		t.Fatalf("an urgent first win must start the bandwidth stage, running=%v pending=%v", d.pkRunning, d.pkPending)
	}
}

// An urgent PK needs only urgentGap since the last download; other long jobs
// keep the full speed_gap, and the 429 pause holds for all.
func TestUrgentGap(t *testing.T) {
	d := testDaemon(t)
	d.cfg.SpeedGap = Duration{15 * time.Minute}
	d.pool.speedNext = t0.Add(10 * time.Minute) // last download 5 minutes ago
	if d.longJobBlocked(t0, gapUrgent) != "" || d.longJobBlocked(t0, gapFull) == "" {
		t.Error("5 minutes after a download, an urgent PK may start and an elective one may not")
	}
	d.pool.speedNext = t0.Add(14 * time.Minute) // 1 minute ago
	if d.longJobBlocked(t0, gapUrgent) == "" {
		t.Error("1 minute after a download, an urgent PK must wait")
	}
	d.pool.speedNext, d.pool.speedPaused = time.Time{}, t0.Add(time.Hour)
	if d.longJobBlocked(t0, gapUrgent) == "" {
		t.Error("the 429 pause holds for urgent PKs too")
	}
}

// A confirmed urgent winner gets its bandwidth stage beyond the daily budget.
func TestUrgentPKBypassesDailyBudget(t *testing.T) {
	for _, tc := range []struct {
		chall float64
		stage bool
	}{{150, true}, {300, false}} {
		d, inc, sb := pkPairFixture(t)
		d.ctx = cancelledCtx()
		d.pool.pk.Day, d.pool.pk.BWUsed = t0.Format("2006-01-02"), 2
		d.pkPending = &pkPending{Challenger: sb.Addr, Incumbent: inc.Addr, At: t0.Add(-10 * time.Minute)}
		r := &pkRun{challOK: 8, incOK: 8}
		for range 8 {
			r.challRTT = append(r.challRTT, tc.chall)
			r.incRTT = append(r.incRTT, 400)
		}
		d.applyPK(jobResult{job: job{kind: jPK, ip: sb.Addr, peer: inc.Addr}, pk: r}, t0)
		if d.pkRunning != tc.stage {
			t.Errorf("challenger %v ms vs 400 ms with the budget spent: bandwidth stage=%v, want %v", tc.chall, d.pkRunning, tc.stage)
		}
	}
}

// While an active is degraded, every standby is probed within
// urgentStandbyEvery, so PK compares against current numbers.
func TestDegradedActiveRefreshesStandbys(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	sb.NextProbe = t0.Add(30 * time.Minute)
	if got := d.pool.standbyEvery(sb); got <= urgentStandbyEvery {
		t.Fatalf("with healthy actives a standby keeps its normal interval, got %v", got)
	}
	inc.State = StateDegraded
	if got := d.pool.standbyEvery(sb); got > urgentStandbyEvery {
		t.Fatalf("with a degraded active the interval must be at most %v, got %v", urgentStandbyEvery, got)
	}
	d.schedule(t0)
	if sb.NextProbe.After(t0.Add(urgentStandbyEvery)) {
		t.Fatalf("a standby due in 30 minutes must be pulled in, next probe %v", sb.NextProbe.Sub(t0))
	}
}

// The challenger counts at the median of its latest successful probes: an
// old fast window does not make a standby look fast now. Probe times are
// fixtures.
func TestPKChallengerUsesRecentProbes(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	for i := range sb.Win {
		sb.Win[i].RTT = 150 // eight fast samples hours ago keep the window median at 150...
	}
	for range 4 {
		sb.Win = append(sb.Win, Obs{OK: true, RTT: 800, Colo: "KIX", T: t0}) // ...but it is slow now
	}
	if sb.medianRTT() != 150 {
		t.Fatalf("fixture: window median %v, want 150", sb.medianRTT())
	}
	if s, _ := d.pool.pickPKPair(t0, nil); s != nil {
		t.Fatalf("a standby that is slow now must not challenge a 400 ms incumbent, got %v", s.Addr)
	}
}

// Among eligible standbys, PK takes the fastest one now, not the first in rank.
func TestPKPicksFastestChallenger(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	for i := range sb.Win {
		sb.Win[i].RTT = 250
	}
	fast := freshStandby("198.18.4.1", "KIX", 120)
	fast.Win[0].OK = false // one failure ranks it after sb
	d.pool.ips[fast.Addr] = fast
	if r := d.pool.rankedStandbys(); r[0] != sb {
		t.Fatal("fixture: sb must rank first")
	}
	if s, i := d.pool.pickPKPair(t0, nil); s != fast || i != inc {
		t.Fatalf("PK must take the fastest eligible standby, got %v", s)
	}
}
