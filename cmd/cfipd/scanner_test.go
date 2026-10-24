package main

import (
	"math/rand"
	"testing"
	"time"
)

func TestBlockKeyCanonical(t *testing.T) {
	if got := BlockKey("172.64.52.137"); got != "172.64.52" {
		t.Fatalf("v4 key = %q", got)
	}
	if got := BlockKey("2a06:98c1:3109:b980:20cd:5db4:5d60:c502"); got != "2a06:98c1:3109::" {
		t.Fatalf("v6 key = %q", got)
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

// TestPruneDropsOnlyUnusableV6Stats: v6 statistics stale for more than 7
// days no longer steer sampling (goodV6 skips them) and are the only ones
// that grow without bound, so prune drops exactly those. v4 statistics are
// bounded by the bundled /24 table and keep steering sampling when stale.
func TestPruneDropsOnlyUnusableV6Stats(t *testing.T) {
	s := NewScanner(defaultConfig(), func(string) bool { return false }, func() float64 { return 200 })
	s.stats["2a06:98c1:3101::"] = &PrefixStat{Prefix: "2a06:98c1:3101::", Probes: 2, OK: 2, LastProbe: t0.Add(-8 * 24 * time.Hour)}
	s.stats["2a06:98c1:3102::"] = &PrefixStat{Prefix: "2a06:98c1:3102::", Probes: 2, OK: 2, LastProbe: t0.Add(-6 * 24 * time.Hour)}
	s.stats["198.18.0"] = &PrefixStat{Prefix: "198.18.0", Probes: 1, LastProbe: t0.Add(-400 * 24 * time.Hour)}
	s.prune(t0)
	if _, ok := s.stats["2a06:98c1:3101::"]; ok {
		t.Fatal("a v6 statistic stale for 8 days must be dropped")
	}
	if _, ok := s.stats["2a06:98c1:3102::"]; !ok {
		t.Fatal("a fresh v6 statistic must stay")
	}
	if _, ok := s.stats["198.18.0"]; !ok {
		t.Fatal("v4 statistics are bounded and must stay however old")
	}
}
