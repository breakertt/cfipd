package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// These fixtures describe behavioral contracts, not production traffic.
func regressionPair(t *testing.T) (*daemon, *IP, *IP) {
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

func TestV2RegressionRecoveryNeedsConsecutiveStableSamples(t *testing.T) {
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

func TestV2RegressionSlowAvailableNodeNeedsQualifiedReplacement(t *testing.T) {
	d, inc, sb := regressionPair(t)
	inc.State, inc.DegradedSince = StateDegraded, t0.Add(-time.Hour)
	if reason := d.pool.checkDown(inc, t0); reason != "" {
		t.Fatalf("fresh trace samples alone must not bypass PK/promotion validation or observation-only period: %s", reason)
	}
	sb.Win, sb.Speed = nil, 0
	if reason := d.pool.checkDown(inc, t0); reason != "" {
		t.Fatalf("a slow available node must keep serving until replacement qualifies: %s", reason)
	}
}

func TestV2RegressionPKCanImproveDegradedButAvailableIncumbent(t *testing.T) {
	d, inc, sb := regressionPair(t)
	inc.State = StateDegraded
	gotSB, gotInc := d.pool.pickPKPair(t0)
	if gotSB != sb || gotInc != inc {
		t.Fatalf("PK should target the slow usable incumbent, got challenger=%v incumbent=%v", gotSB, gotInc)
	}
}

func TestV2RegressionPKCannotQualifyWithFailedHTTPChecks(t *testing.T) {
	r := &pkRun{challOK: 6, incOK: 8, challRTT: []float64{100, 100, 100, 100, 100, 100}, incRTT: []float64{300, 300, 300, 300, 300, 300, 300, 300}}
	if ok, _ := r.pass(defaultConfig()); ok {
		t.Fatal("a final-qualified challenger cannot silently accept 2 failed/wrong-colo checks out of 8")
	}
}

func TestV2RegressionMissingBandwidthReferenceIsNotAWin(t *testing.T) {
	d, inc, sb := regressionPair(t)
	d.pool.pk.ObserveUntil = t0.Add(-time.Hour)
	d.applySpeed(jobResult{job: job{kind: jSpeed, ip: sb.Addr, peer: inc.Addr}, ok: true,
		speed: SpeedResult{Mbps: 100, Bytes: 1024, Colo: "KIX"}, speed2: SpeedResult{Err: "status 403"}}, t0)
	if sb.Role != RoleStandby || inc.Role != RoleActive {
		t.Fatal("an unavailable incumbent bandwidth reference cannot authorize an elective swap")
	}
}

func TestV2RegressionGenerationZeroAlsoExpires(t *testing.T) {
	d, inc, sb := regressionPair(t)
	d.pool.gen = 0
	d.pool.cooldown(inc, "fixture failure", t0)
	d.apply(jobResult{job: job{kind: jPromote, ip: sb.Addr, gen: 0}, ok: true, med: 150,
		speed: SpeedResult{Mbps: 100}, probe: okProbe(150, "KIX")})
	if sb.Role != RoleStandby {
		t.Fatal("a job from generation zero must be rejected after the pool generation changes")
	}
}

func TestV2RegressionV1LoadBoundsAndUntrustedStandbyWindow(t *testing.T) {
	d := testDaemon(t)
	now := time.Now()
	old := snapshot{SavedAt: now, IPs: map[string]*IP{}, Prefixes: map[string]*PrefixStat{}}
	for i := range 5000 {
		addr := fmt.Sprintf("172.20.%d.%d", i/254, i%254+1)
		old.IPs[addr] = &IP{Addr: addr, Role: RoleCandidate, Colo: "SIN", FirstSeen: now.Add(-24 * time.Hour), LastSeen: now}
	}
	for i := range 12 {
		addr := fmt.Sprintf("198.18.%d.1", i)
		old.IPs[addr] = &IP{Addr: addr, Role: RoleStandby, Colo: "NRT", FirstSeen: now.Add(-24 * time.Hour), LastSeen: now,
			Win: []Obs{{OK: true, RTT: 170, Colo: "NRT", T: now}, {OK: true, RTT: 180, Colo: "NRT", T: now}, {OK: true, RTT: 190, Colo: "NRT", T: now}}}
	}
	b, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.store.dir, "state.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.store.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	if n := len(d.pool.candidates()); n > d.cfg.CandidateCap {
		t.Errorf("loaded %d candidates, cap=%d", n, d.cfg.CandidateCap)
	}
	if n := len(d.pool.standbys()); n > d.cfg.StandbyMax {
		t.Errorf("loaded %d standbys, cap=%d", n, d.cfg.StandbyMax)
	}
	for _, sb := range d.pool.standbys() {
		if len(sb.Win) != 0 {
			t.Error("v1 standby window with synthesized equal timestamps must be revalidated")
			break
		}
	}
}

func TestV2RegressionRejectedDiscoveryDoesNotResetUsefulYield(t *testing.T) {
	d := testDaemon(t)
	d.cfg.CandidateCap = 1
	d.pool.ensure("198.18.0.1", t0)
	d.scan.noYield = 19
	d.apply(jobResult{job: job{kind: jDiscover, ip: "198.18.1.1"}, probe: okProbe(150, "NRT")})
	if d.scan.noYield < 20 {
		t.Fatal("an Asian discovery rejected by capacity has not produced useful candidate work")
	}
}

func TestV2RegressionCountsPhysicalPromotionProbes(t *testing.T) {
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
	r := d.execPromote(context.Background(), job{kind: jPromote, ip: sb.Addr, limit: 1000, gen: d.pool.gen})
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
