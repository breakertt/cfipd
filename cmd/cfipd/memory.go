package main

import (
	"bufio"
	"io"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
)

// memoryLimit returns the Go soft memory limit to apply, or 0 to leave the
// runtime alone. A configured memory_limit_mb is fixed. Otherwise the limit
// follows the host: the memory the process uses now plus a headroom of a
// quarter of MemAvailable, but at least 4 MiB and a quarter of the current
// use so the collector never runs without room, and never under 8 MiB. Under
// memory pressure the limit shrinks, and the runtime collects harder and
// returns memory to the system; with plenty free it stays out of the way.
// GOMEMLIMIT in the environment always wins.
func memoryLimit(cfgMB int, envSet bool, inUse, avail uint64, ok bool) int64 {
	switch {
	case envSet:
		return 0
	case cfgMB > 0:
		return int64(cfgMB) << 20
	case !ok:
		return 0
	}
	return max(8<<20, int64(inUse+max(avail/4, 4<<20, inUse/4)))
}

// parseMemAvailable reads MemAvailable from /proc/meminfo text.
func parseMemAvailable(r io.Reader) (uint64, bool) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) >= 2 && f[0] == "MemAvailable:" {
			kb, err := strconv.ParseUint(f[1], 10, 64)
			return kb << 10, err == nil
		}
	}
	return 0, false
}

// goMemoryInUse is the memory the soft limit accounts for: everything the Go
// runtime maps, minus what it has already returned to the system.
func goMemoryInUse() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64() - s[1].Value.Uint64()
}

// tuneMemory applies memoryLimit; the daemon calls it at start and every
// minute. It returns the limit now in force (see memoryLimitMB).
func tuneMemory(cfg *Config) int64 {
	var avail uint64
	var ok bool
	if f, err := os.Open("/proc/meminfo"); err == nil {
		avail, ok = parseMemAvailable(f)
		f.Close()
	}
	if limit := memoryLimit(cfg.MemoryLimitMB, os.Getenv("GOMEMLIMIT") != "", goMemoryInUse(), avail, ok); limit > 0 {
		debug.SetMemoryLimit(limit)
	}
	return memoryLimitMB()
}

// memoryLimitMB reads the soft memory limit in force in MiB, 0 for none.
func memoryLimitMB() int64 {
	if limit := debug.SetMemoryLimit(-1); limit != math.MaxInt64 {
		return limit >> 20
	}
	return 0
}
