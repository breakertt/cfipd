package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestMemoryLimitPolicy: a configured limit is fixed; otherwise the limit is
// the current use plus a headroom of a quarter of MemAvailable, but at least
// 4 MiB and a quarter of the current use, and never under 8 MiB; GOMEMLIMIT or
// an unreadable MemAvailable leaves the runtime alone.
func TestMemoryLimitPolicy(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		name         string
		cfgMB        int
		env, ok      bool
		inUse, avail uint64
		want         int64
	}{
		{"configured", 16, false, true, 10 * mib, 400 * mib, 16 * mib},
		{"env wins", 16, true, true, 10 * mib, 400 * mib, 0},
		{"auto", 0, false, true, 10 * mib, 40 * mib, 20 * mib},
		{"auto floor", 0, false, true, 4 * mib, 8 * mib, 8 * mib},
		{"tiny available", 0, false, true, 12 * mib, 2 * mib, 16 * mib},
		{"large use", 0, false, true, 40 * mib, 0, 50 * mib},
		{"no meminfo", 0, false, false, 10 * mib, 0, 0},
	} {
		if got := memoryLimit(tc.cfgMB, tc.env, tc.inUse, tc.avail, tc.ok); got != tc.want {
			t.Errorf("%s: limit %d MiB, want %d MiB", tc.name, got/mib, tc.want/mib)
		}
	}
}

// TestParseMemAvailable reads MemAvailable from /proc/meminfo text.
func TestParseMemAvailable(t *testing.T) {
	text := "MemTotal:         126976 kB\nMemFree:           8192 kB\nMemAvailable:      40960 kB\n"
	if got, ok := parseMemAvailable(strings.NewReader(text)); !ok || got != 40960*1024 {
		t.Fatalf("got %d, %v", got, ok)
	}
	if _, ok := parseMemAvailable(strings.NewReader("MemTotal: 1 kB\n")); ok {
		t.Fatal("a kernel without MemAvailable must report not ok")
	}
}

// The host is short of memory when it has less free than twice what cfipd uses.
func TestLowMemory(t *testing.T) {
	const mb = 1 << 20
	for _, tc := range []struct {
		inUse, avail uint64
		ok, low      bool
	}{
		{11 * mb, 20 * mb, true, true},
		{11 * mb, 30 * mb, true, false},
		{11 * mb, 0, false, false}, // no /proc/meminfo: never low
	} {
		if got := lowMemory(tc.inUse, tc.avail, tc.ok); got != tc.low {
			t.Errorf("in use %d MB, available %d MB: low=%v, want %v", tc.inUse/mb, tc.avail/mb, got, tc.low)
		}
	}
}

// While memory is low, shedMemory drops prefix statistics not probed for a day
// unless they are good, and candidates over a quarter of candidate_cap; the
// lower cap also holds at admission. With enough memory, v4 statistics stay.
// Statistics and candidates are fixtures.
func TestShedMemory(t *testing.T) {
	d := testDaemon(t)
	d.cfg.CandidateCap = 8
	now := t0
	stats := map[string]*PrefixStat{
		"198.18.1":     {Prefix: "198.18.1", Probes: 10, LastProbe: now.Add(-48 * time.Hour)},                     // old, dead
		"198.18.2":     {Prefix: "198.18.2", Probes: 10, OK: 10, ColoOK: 10, LastProbe: now.Add(-48 * time.Hour)}, // old, good
		"198.18.3":     {Prefix: "198.18.3", Probes: 10, LastProbe: now.Add(-time.Hour)},                          // recent
		"2001:db8:1::": {Prefix: "2001:db8:1::", Probes: 3, LastProbe: now.Add(-48 * time.Hour)},                  // old v6
	}
	for k, v := range stats {
		d.scan.stats[k] = v
	}
	for i := range 6 {
		addr := fmt.Sprintf("198.19.%d.1", i)
		d.pool.ips[addr] = &IP{Addr: addr, Role: RoleCandidate, FirstSeen: now, LastSeen: now}
	}

	d.scan.prune(now, false)
	if len(d.scan.stats) != 4 {
		t.Fatalf("with enough memory, every statistic younger than 7 days stays, got %d", len(d.scan.stats))
	}

	d.lowMem = true
	d.shedMemory(now)
	for k, keep := range map[string]bool{"198.18.1": false, "198.18.2": true, "198.18.3": true, "2001:db8:1::": false} {
		if _, ok := d.scan.stats[k]; ok != keep {
			t.Errorf("low memory: statistic %s kept=%v, want %v", k, ok, keep)
		}
	}
	if n := len(d.pool.candidates()); n != 2 {
		t.Errorf("low memory: %d candidates, want a quarter of candidate_cap (2)", n)
	}
	if d.candidateRoom("198.20.0.1") {
		t.Error("low memory: admission must use the lower candidate cap")
	}
	d.lowMem = false
	if !d.candidateRoom("198.20.0.1") {
		t.Error("with enough memory, the full candidate cap must return")
	}
}
