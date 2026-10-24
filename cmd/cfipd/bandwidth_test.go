package main

import (
	"testing"
	"time"
)

// TestBandwidthRecheckDueActives: actives are re-measured one at a time once
// their latest sample is older than SpeedRecheck; a fresh sample waits, and no
// recheck starts while another bandwidth download may run.
func TestBandwidthRecheckDueActives(t *testing.T) {
	d := testDaemon(t)
	d.ctx = cancelledCtx()
	d.cfg.SpeedRecheck = Duration{12 * time.Hour}
	old, never, fresh := sinActive("198.18.0.1", 300), sinActive("198.18.1.1", 300), sinActive("198.18.2.1", 300)
	old.Speed, old.SpeedAt = 80, t0.Add(-13*time.Hour)
	fresh.Speed, fresh.SpeedAt = 80, t0.Add(-time.Hour)
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
		d.rechecking = false // fixture: the previous recheck finished
	}
	if len(d.recheckAt) != 2 || d.recheckAt[old.Addr].IsZero() || d.recheckAt[never.Addr].IsZero() {
		t.Fatalf("exactly the two due actives must be rechecked, got %v", d.recheckAt)
	}
}

// TestRecheckBelowFloorCutsRelayWeight: a fresh sample under the promotion
// floor is recorded truthfully and cuts the active's relay weight; a failed
// download keeps the previous sample.
func TestRecheckBelowFloorCutsRelayWeight(t *testing.T) {
	d := testDaemon(t)
	slow, peer := sinActive("198.18.0.1", 300), sinActive("198.18.1.1", 300)
	slow.Speed, slow.SpeedAt = 80, t0.Add(-13*time.Hour)
	d.pool.ips[slow.Addr], d.pool.ips[peer.Addr] = slow, peer
	d.rechecking = true
	d.apply(jobResult{job: job{kind: jRecheck, ip: slow.Addr, gen: slow.Gen}, ok: true,
		speed: SpeedResult{Mbps: 2, Bytes: 2_500_000, Colo: "SIN"}})
	if d.rechecking || slow.Speed != 2 || !slow.SpeedAt.After(t0) || d.pool.dayBytes != 2_500_000 {
		t.Fatalf("recheck sample not recorded: speed=%v at=%v bytes=%d", slow.Speed, slow.SpeedAt, d.pool.dayBytes)
	}
	w := map[string]float64{}
	for _, tg := range d.relaySnap.Load().([]relayTarget) {
		w[tg.Addr] = tg.Weight
	}
	if w[slow.Addr]*4 != w[peer.Addr] {
		t.Fatalf("a below-floor active must get a quarter of the relay weight, got %v", w)
	}
	at := slow.SpeedAt
	d.apply(jobResult{job: job{kind: jRecheck, ip: slow.Addr, gen: slow.Gen},
		speed: SpeedResult{Err: "status 403"}})
	if slow.Speed != 2 || !slow.SpeedAt.Equal(at) {
		t.Fatal("a failed download must not overwrite the latest sample")
	}
}

// TestLowBandwidthActiveReplacedByQualifiedStandby: a below-floor active keeps
// serving until a standby passes the full promotion test, then swaps one for
// one. Each below-floor sample triggers at most one replacement attempt.
func TestLowBandwidthActiveReplacedByQualifiedStandby(t *testing.T) {
	d, _, sb := pkPairFixture(t)
	d.ctx = cancelledCtx()
	low := d.pool.ips["198.18.0.1"]
	low.Speed, low.SpeedAt = 2, t0
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

	low.SpeedAt = t0.Add(12 * time.Hour) // the next recheck is still under the floor
	sb2.LastSeen = t0.Add(12 * time.Hour)
	d.replaceLowBandwidth(t0.Add(12 * time.Hour))
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
