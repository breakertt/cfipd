package main

import (
	"strings"
	"testing"
)

// TestMemoryLimitPolicy: a configured limit is fixed; otherwise the limit is
// the current use plus a quarter of MemAvailable, at least 8 MiB; GOMEMLIMIT
// or an unreadable MemAvailable leaves the runtime alone.
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
