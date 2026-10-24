package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestBandwidthRecheckDueActives: actives are re-measured one at a time once
// their SpeedDue passes; a never-measured active is due at once, and no
// recheck starts while another long job may download.
func TestBandwidthRecheckDueActives(t *testing.T) {
	d := testDaemon(t)
	d.ctx = cancelledCtx()
	d.cfg.SpeedRecheck = Duration{12 * time.Hour}
	old, never, fresh := sinActive("198.18.0.1", 300), sinActive("198.18.1.1", 300), sinActive("198.18.2.1", 300)
	old.Speed, old.SpeedAt, old.SpeedDue = 80, t0.Add(-13*time.Hour), t0.Add(-time.Hour)
	fresh.Speed, fresh.SpeedAt, fresh.SpeedDue = 80, t0.Add(-time.Hour), t0.Add(11*time.Hour)
	for _, a := range []*IP{old, never, fresh} {
		d.pool.ips[a.Addr] = a
	}
	d.pkRunning = true
	d.recheckBandwidth(t0)
	if d.rechecking {
		t.Fatal("a recheck must not overlap a PK bandwidth stage")
	}
	d.pkRunning = false
	for range 3 {
		d.recheckBandwidth(t0)
		d.rechecking = false // fixture: the previous recheck ended without a result
	}
	for _, a := range []*IP{old, never} {
		if !a.SpeedDue.After(t0) {
			t.Fatalf("%s: a dispatched recheck must push SpeedDue out, got %v", a.Addr, a.SpeedDue)
		}
	}
	if !fresh.SpeedDue.Equal(t0.Add(11 * time.Hour)) {
		t.Fatal("an active whose SpeedDue is in the future must wait")
	}
}

// TestBelowFloorNeedsConfirmation: one sample under the promotion floor only
// schedules a confirmation about an hour later; a second consecutive low
// sample confirms it and cuts the relay weight. A failed download keeps the
// latest sample, and a sample at the floor clears the streak.
func TestBelowFloorNeedsConfirmation(t *testing.T) {
	d := testDaemon(t)
	d.cfg.SpeedRecheck = Duration{12 * time.Hour}
	slow, peer := sinActive("198.18.0.1", 300), sinActive("198.18.1.1", 300)
	peer.Speed, peer.SpeedAt = 80, t0 // the link reaches the floor
	d.pool.ips[slow.Addr], d.pool.ips[peer.Addr] = slow, peer
	recheck := func(s SpeedResult) {
		d.rechecking = true
		d.apply(jobResult{job: job{kind: jRecheck, ip: slow.Addr, gen: slow.Gen}, ok: s.Err == "", speed: s})
	}
	weights := func() map[string]float64 {
		w := map[string]float64{}
		for _, tg := range d.relaySnap.Load().([]relayTarget) {
			w[tg.Addr] = tg.Weight
		}
		return w
	}

	recheck(SpeedResult{Mbps: 2, Bytes: 2_500_000, Colo: "SIN"})
	if d.rechecking || slow.Speed != 2 || slow.belowFloor() || d.pool.dayBytes != 2_500_000 {
		t.Fatalf("a first low sample must be recorded but not confirmed: speed=%v n=%d bytes=%d",
			slow.Speed, slow.BelowFloorN, d.pool.dayBytes)
	}
	if until := time.Until(slow.SpeedDue); until > time.Hour || until < 45*time.Minute {
		t.Fatalf("the confirmation recheck must come about an hour later, got %v", until)
	}
	if w := weights(); w[slow.Addr] != w[peer.Addr] {
		t.Fatalf("an unconfirmed low sample must not cut the relay weight, got %v", w)
	}
	recheck(SpeedResult{Err: "status 403"})
	if slow.Speed != 2 || slow.BelowFloorN != 1 {
		t.Fatal("a failed download must keep the latest sample")
	}
	recheck(SpeedResult{Mbps: 3, Bytes: 3_000_000, Colo: "SIN"})
	if !slow.belowFloor() {
		t.Fatal("two consecutive low samples must confirm the active is below the floor")
	}
	if w := weights(); w[slow.Addr]*4 != w[peer.Addr] {
		t.Fatalf("a confirmed below-floor active must get a quarter of the relay weight, got %v", w)
	}
	recheck(SpeedResult{Mbps: 60, Bytes: 60_000_000, Colo: "SIN"})
	if slow.belowFloor() || slow.BelowFloorN != 0 {
		t.Fatal("a sample at or above the floor must clear the streak")
	}
}

// TestLowBandwidthActiveReplacedByQualifiedStandby: a confirmed below-floor
// active keeps serving until a standby passes the full promotion test, then
// swaps one for one. Each below-floor sample triggers at most one attempt.
func TestLowBandwidthActiveReplacedByQualifiedStandby(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	low := d.pool.ips["198.18.0.1"]
	low.Speed, low.SpeedAt, low.BelowFloorN = 2, t0, 2
	d.pool.ips["198.18.1.1"].Speed, d.pool.ips["198.18.1.1"].SpeedAt = 80, t0 // the link reaches the floor
	sb2 := freshStandby("198.18.4.1", "KIX", 160)
	d.pool.ips[sb2.Addr] = sb2

	d.replaceLowBandwidth(t0)
	if !sb.Promoting || !d.promoting {
		t.Fatal("a below-floor active must start a replacement promotion")
	}
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: sb.Gen, peer: low.Addr, peerGen: low.Gen},
		err: "speed 5.0Mbps < 30.0Mbps"})
	if low.Role != RoleActive {
		t.Fatal("a failed replacement must leave the below-floor active serving")
	}
	d.replaceLowBandwidth(t0)
	if sb2.Promoting {
		t.Fatal("one below-floor sample must trigger at most one replacement attempt")
	}

	later := t0.Add(12 * time.Hour)
	low.SpeedAt, low.BelowFloorN = later, 3 // the next recheck is still under the floor
	sb2.LastSeen = later
	d.replaceLowBandwidth(later)
	if !sb2.Promoting {
		t.Fatal("a new below-floor sample must allow another replacement attempt")
	}
	d.apply(jobResult{job: job{kind: jPromote, ip: sb2.Addr, gen: sb2.Gen, peer: low.Addr, peerGen: low.Gen},
		ok: true, med: 160, speed: SpeedResult{Mbps: 100}, probe: okProbe(160, "KIX")})
	if sb2.Role != RoleActive || low.Role != RoleStandby || len(d.pool.actives()) != 3 {
		t.Fatalf("a qualified replacement must swap one for one: sb2=%s low=%s actives=%d",
			sb2.Role, low.Role, len(d.pool.actives()))
	}
}

// TestStaleReplacementClearsPromoting: a replacement test whose standby left
// the pool and whose incumbent changed generation is stale, and it must still
// release the promotion slot.
func TestStaleReplacementClearsPromoting(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	low := d.pool.ips["198.18.0.1"]
	low.Speed, low.SpeedAt, low.BelowFloorN = 2, t0, 2
	sb.Promoting, d.promoting = true, true
	j := job{kind: jPromote, ip: sb.Addr, gen: sb.Gen, peer: low.Addr, peerGen: low.Gen}
	d.pool.remove(sb.Addr)                      // evicted as the worst standby mid-test
	d.pool.cooldown(low, "fixture failure", t0) // the incumbent failed mid-test
	d.apply(jobResult{job: j, ok: true, med: 150, speed: SpeedResult{Mbps: 100}})
	if d.promoting {
		t.Fatal("a stale promotion result must release the promotion slot")
	}
}

// TestRateLimitPausesDownloads: downloads keep SpeedGap apart, and an HTTP 429
// from the bandwidth endpoint pauses every download for at least an hour.
func TestRateLimitPausesDownloads(t *testing.T) {
	d := testDaemon(t)
	a := sinActive("198.18.0.1", 300)
	d.pool.ips[a.Addr] = a
	d.rechecking = true
	d.apply(jobResult{job: job{kind: jRecheck, ip: a.Addr, gen: a.Gen}, ok: true,
		speed: SpeedResult{Mbps: 80, Bytes: 80_000_000, Colo: "SIN", Status: 200}})
	if until := time.Until(d.pool.speedNext); until > d.cfg.SpeedGap.Duration || until < d.cfg.SpeedGap.Duration-time.Minute {
		t.Fatalf("the next download must wait SpeedGap, got %v", until)
	}
	d.rechecking = true
	d.apply(jobResult{job: job{kind: jRecheck, ip: a.Addr, gen: a.Gen},
		speed: SpeedResult{Err: "status 429", Status: 429}})
	if time.Until(d.pool.speedPaused) < 59*time.Minute {
		t.Fatalf("HTTP 429 must pause downloads for at least an hour, paused until %v", d.pool.speedPaused)
	}
	if d.longJobBlocked(time.Now(), false) == "" {
		t.Fatal("long jobs must wait out the pause")
	}
}

// TestFailedConfirmationRetriesWithinTheHour: when the confirmation download
// of a first low sample fails, the retry keeps the confirmation schedule.
func TestFailedConfirmationRetriesWithinTheHour(t *testing.T) {
	d := testDaemon(t)
	d.ctx = cancelledCtx()
	d.cfg.SpeedRecheck = Duration{12 * time.Hour}
	a := sinActive("198.18.0.1", 300)
	a.Speed, a.SpeedAt, a.BelowFloorN, a.SpeedDue = 2, t0.Add(-time.Hour), 1, t0
	d.pool.ips[a.Addr] = a
	d.recheckBandwidth(t0)
	if wait := a.SpeedDue.Sub(t0); !d.rechecking || wait <= 0 || wait > time.Hour {
		t.Fatalf("a pending confirmation must retry within the hour, got %v", wait)
	}
}

// TestFloorActsOnlyWhereReachable: a below-floor active loses relay weight and
// gets a replacement test only while another active shows that the link
// reaches the floor. Where every measured active is under it, including a PK
// winner under the floor waiver, nobody can meet it: weights stay
// latency-based and no futile replacement test runs.
func TestFloorActsOnlyWhereReachable(t *testing.T) {
	d, inc, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	for _, a := range d.pool.actives() {
		a.Speed, a.SpeedAt, a.BelowFloorN = 20, t0, 2 // slow link: every active under 30 Mbps
	}
	sb.Speed, sb.SpeedAt, sb.BelowFloorN = 24, t0, 1 // an earlier sub-floor sample
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 25, Colo: "KIX"}, speed2: SpeedResult{Mbps: 18, Colo: "HKG"}}, t0)
	if sb.Role != RoleActive || !sb.belowFloor() {
		t.Fatalf("fixture: the waiver must let the faster endpoint win with a sub-floor streak, role=%s n=%d", sb.Role, sb.BelowFloorN)
	}
	weight := func(addr string) float64 {
		d.updateRelaySnap()
		for _, tg := range d.relaySnap.Load().([]relayTarget) {
			if tg.Addr == addr {
				return tg.Weight
			}
		}
		return 0
	}
	if w := weight(sb.Addr); w != 1/sb.medianRTT() {
		t.Fatalf("no active reaches the floor, so the winner must keep its latency weight, got %v want %v", w, 1/sb.medianRTT())
	}
	d.replaceLowBandwidth(t0)
	if d.promoting {
		t.Fatal("no replacement test may start where no active reaches the floor")
	}
	fast := d.pool.ips["198.18.0.1"]
	fast.Speed, fast.BelowFloorN = 80, 0 // now one active shows the link can do it
	if w := weight(sb.Addr); w != 1/sb.medianRTT()/4 {
		t.Fatalf("with the floor reachable, a confirmed below-floor active must get a quarter weight, got %v", w)
	}
	d.replaceLowBandwidth(t0)
	if !d.promoting {
		t.Fatal("with the floor reachable, a confirmed below-floor active must get a replacement test")
	}
}

// TestRecheckLogMatchesAction: a confirmed below-floor result logs that a
// replacement is sought only where the floor is reachable; on a slow link it
// logs that nothing changes.
func TestRecheckLogMatchesAction(t *testing.T) {
	d := testDaemon(t)
	a := sinActive("198.18.0.1", 300)
	a.Speed, a.SpeedAt, a.BelowFloorN = 20, t0, 1
	d.pool.ips[a.Addr] = a
	d.rechecking = true
	d.apply(jobResult{job: job{kind: jRecheck, ip: a.Addr, gen: a.Gen}, ok: true,
		speed: SpeedResult{Mbps: 21, Bytes: 1, Colo: "SIN"}})
	d.store.Close()
	b, err := os.ReadFile(filepath.Join(d.store.dir, "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if log := string(b); strings.Contains(log, "seeking a replacement") || !strings.Contains(log, "nothing changes") {
		t.Fatalf("on a slow link the event must say nothing changes:\n%s", log)
	}
}
