package main

import (
	"bufio"
	"io"
	"log"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
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

// lowMemory reports memory pressure: the host has less free memory than twice
// what cfipd uses. Without /proc/meminfo it never reports pressure.
func lowMemory(inUse, avail uint64, ok bool) bool {
	return ok && avail < 2*inUse
}

// tuneMemory applies memoryLimit; the daemon calls it at start and every
// minute. It returns the limit now in force (see memoryLimitMB) and whether
// memory is low.
func tuneMemory(cfg *Config) (int64, bool) {
	var avail uint64
	var ok bool
	if f, err := os.Open("/proc/meminfo"); err == nil {
		avail, ok = parseMemAvailable(f)
		f.Close()
	}
	inUse := goMemoryInUse()
	if limit := memoryLimit(cfg.MemoryLimitMB, os.Getenv("GOMEMLIMIT") != "", inUse, avail, ok); limit > 0 {
		debug.SetMemoryLimit(limit)
	}
	return memoryLimitMB(), lowMemory(inUse, avail, ok)
}

// shedMemory runs each minute while memory is low. Only the prefix statistics
// (about 1 MB with the full v4 table) and the candidates are large enough to
// matter: it drops statistics not probed for a day unless they are good, and
// candidates over the lowered cap. Dropped statistics are learning data, most
// of them about dead prefixes; sampling treats those prefixes as unexplored and
// relearns them by probing. On the first run of a low period it also returns
// the freed memory to the system and logs; later runs only keep the bounds,
// and the soft memory limit returns memory from then on.
func (d *daemon) shedMemory(now time.Time, first bool) {
	stats := len(d.scan.stats)
	d.pool.pruneCandidates(now, d.candidateCap())
	d.scan.prune(now, true)
	if first {
		debug.FreeOSMemory()
		log.Printf("low memory: dropped %d prefix statistics, candidate cap now %d", stats-len(d.scan.stats), d.candidateCap())
	}
}

// candidateCap is candidate_cap, or a quarter of it while memory is low.
func (d *daemon) candidateCap() int {
	if d.lowMem {
		return max(1, d.cfg.CandidateCap/4)
	}
	return d.cfg.CandidateCap
}

// memoryLimitMB reads the soft memory limit in force in MiB, 0 for none.
func memoryLimitMB() int64 {
	if limit := debug.SetMemoryLimit(-1); limit != math.MaxInt64 {
		return limit >> 20
	}
	return 0
}
