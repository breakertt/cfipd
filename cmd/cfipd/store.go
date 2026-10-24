package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// histRotateSize bounds each history.jsonl segment and histKeep the number of
// rotated segments, so history uses at most about (histKeep+1)*histRotateSize
// on disk. Rotation prunes the oldest archives beyond histKeep and logs each.
const (
	histRotateSize = 8 << 20
	histKeep       = 3
)

type Store struct {
	dir    string
	hist   *os.File
	histSz int64
	writes int
}

type pkPersist struct {
	ObserveUntil time.Time `json:"observe_until"`
	Day          string    `json:"day"`
	BWUsed       int       `json:"bw_used"`
}

type snapshot struct {
	SavedAt   time.Time              `json:"saved_at"`
	IPs       map[string]*IP         `json:"ips"`
	Prefixes  map[string]*PrefixStat `json:"prefixes"`
	PK        *pkPersist             `json:"pk,omitempty"`
	Day       string                 `json:"day,omitempty"`
	DayCounts map[string]int         `json:"day_counts,omitempty"`
	DayBytes  int64                  `json:"day_bytes,omitempty"`
	SpeedNext time.Time              `json:"speed_next,omitempty"`
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, "history.jsonl")
	if st, err := os.Stat(p); err == nil && st.Size() > histRotateSize {
		if err := rotateFile(dir, p); err != nil {
			log.Printf("history rotate at open: %v (continuing append)", err)
		}
	}
	h, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir, hist: h}
	if st, err := h.Stat(); err == nil {
		s.histSz = st.Size() // track real size, not zero, so rotation stays honest
	}
	return s, nil
}

// rotateFile renames p aside under a unique name, then prunes old archives.
func rotateFile(dir, p string) error {
	for i := range 100 {
		name := "history-" + time.Now().Format("20060102-150405") + ".jsonl"
		if i > 0 {
			name = fmt.Sprintf("history-%s-%d.jsonl", time.Now().Format("20060102-150405"), i)
		}
		dst := filepath.Join(dir, name)
		if _, err := os.Stat(dst); err == nil {
			continue // never overwrite an existing archive
		}
		if err := os.Rename(p, dst); err != nil {
			return err
		}
		log.Printf("history rotated to %s", dst)
		pruneArchives(dir)
		return nil
	}
	return fmt.Errorf("no free archive name")
}

// pruneArchives removes rotated segments beyond the newest histKeep. Archive
// names start with the rotation time, so lexical order is age order to the
// second.
func pruneArchives(dir string) {
	old, err := filepath.Glob(filepath.Join(dir, "history-*.jsonl"))
	if err != nil || len(old) <= histKeep {
		return
	}
	sort.Strings(old)
	for _, p := range old[:len(old)-histKeep] {
		if err := os.Remove(p); err != nil {
			log.Printf("history prune: %v", err)
			continue
		}
		log.Printf("history pruned %s (keeping newest %d archives)", p, histKeep)
	}
}

func (s *Store) Load(pool *Pool, scan *Scanner) error {
	b, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("state.json: %w", err)
	}
	now := time.Now()
	for addr, ip := range snap.IPs {
		ip.Promoting = false
		if ip.Role == RoleQuarantine {
			// v1 quarantine migrates to a bounded candidate cooldown; the
			// v1 exponential penalty (up to 12h) is collapsed to one Cooldown.
			ip.Role = RoleCandidate
			ip.CooldownUntil = now.Add(pool.cfg.Cooldown.Duration)
			if !ip.QuarUntil.IsZero() && ip.QuarUntil.Before(ip.CooldownUntil) {
				ip.CooldownUntil = ip.QuarUntil
			}
			ip.NextProbe = ip.CooldownUntil
			ip.CycleSince = now
			ip.Win = nil
			ip.ConsecFail, ip.ConsecOK, ip.HardFails, ip.ColoBad, ip.Spikes = 0, 0, 0, 0, 0
		}
		ip.QuarUntil = time.Time{}
		pool.ips[addr] = ip
	}
	migrateBounds(pool, now, snap.PK == nil)
	for k, st := range snap.Prefixes {
		// Merge legacy keys into the canonical BlockKey form (e.g. v1 sampler
		// keys "172.64.52.0" fold into "172.64.52"); never written again.
		ck := k
		if a, err := netip.ParseAddr(k); err == nil {
			ck = BlockKey(a.String())
		}
		if cur, ok := scan.stats[ck]; ok && cur != st {
			cur.Probes += st.Probes
			cur.OK += st.OK
			cur.ColoOK += st.ColoOK
			cur.RTTSum += st.RTTSum
			cur.RTTCount += st.RTTCount
			if st.LastProbe.After(cur.LastProbe) {
				cur.LastProbe = st.LastProbe
			}
			continue
		}
		st.Prefix = ck
		scan.stats[ck] = st
	}
	if snap.PK == nil {
		// First v2 start: observe and log PK decisions for a day before any
		// elective swap. Survives restart via the snapshot.
		snap.PK = &pkPersist{ObserveUntil: now.Add(24 * time.Hour)}
		log.Printf("PK observe-only until %s", snap.PK.ObserveUntil.Format(time.RFC3339))
	}
	pool.pk = snap.PK
	pool.speedNext = snap.SpeedNext
	if snap.Day == now.Format("2006-01-02") {
		pool.day, pool.dayBytes = snap.Day, snap.DayBytes
		if snap.DayCounts != nil {
			pool.dayCounts = snap.DayCounts
		}
	}
	return nil
}

// migrateBounds enforces v2 caps on a freshly loaded v1 snapshot before any
// request is served. Actives are preserved untouched. Legacy standby windows
// carry synthesized equal timestamps (v1 stamped them all at promotion time)
// and are untrusted: reset for revalidation, never given fictional times.
// FirstSeen (first-ever discovery) is kept; the validation cycle restarts.
func migrateBounds(pool *Pool, now time.Time, legacy bool) {
	for _, sb := range pool.standbys() {
		if legacy && synthesizedWin(sb.Win) {
			sb.Win = nil
		}
		if legacy {
			sb.NextProbe = now // revalidate untrusted legacy evidence promptly
		}
	}
	if sbs := pool.rankedStandbys(); len(sbs) > pool.cfg.StandbyMax {
		for _, sb := range sbs[pool.cfg.StandbyMax:] {
			pool.remove(sb.Addr)
		}
	}
	cands := pool.candidates()
	sort.Slice(cands, func(i, j int) bool { return cands[i].LastSeen.After(cands[j].LastSeen) })
	kept, perPrefix := 0, map[string]int{}
	for _, c := range cands {
		if legacy && !c.CooldownUntil.After(now) && !pool.cfg.coloAllowed(c.Colo) {
			pool.remove(c.Addr)
			continue
		}
		key := BlockKey(c.Addr)
		if kept >= pool.cfg.CandidateCap || perPrefix[key] >= pool.cfg.CandidatePerPrefix {
			pool.remove(c.Addr)
			continue
		}
		kept++
		perPrefix[key]++
		if legacy && !c.CooldownUntil.After(now) {
			c.Win = nil // untrusted legacy evidence: revalidate
			c.CycleSince = now
			c.NextProbe = now
		}
	}
}

// synthesizedWin detects v1 promotion-time backfill: every observation
// stamped with the identical instant.
func synthesizedWin(win []Obs) bool {
	if len(win) < 2 {
		return false
	}
	for _, o := range win[1:] {
		if !o.T.Equal(win[0].T) {
			return false
		}
	}
	return true
}

func (s *Store) Save(pool *Pool, scan *Scanner) error {
	snap := snapshot{SavedAt: time.Now(), IPs: pool.ips, Prefixes: scan.stats, PK: pool.pk,
		Day: pool.day, DayCounts: pool.dayCounts, DayBytes: pool.dayBytes, SpeedNext: pool.speedNext}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "state.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "state.json"))
}

// Log appends one JSON line to history.jsonl. Called only from the event loop.
// Oversized segments rotate aside and archives beyond histKeep are pruned.
// Rotation failures are reported and logging continues on the current handle,
// never silently lost.
func (s *Store) Log(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	n, werr := s.hist.Write(append(b, '\n'))
	if werr != nil {
		log.Printf("history write: %v", werr)
	}
	s.histSz += int64(n)
	if s.writes++; s.writes%256 == 0 && s.histSz > histRotateSize {
		s.rotate()
	}
}

func (s *Store) rotate() {
	p := filepath.Join(s.dir, "history.jsonl")
	old := s.hist
	if err := rotateFile(s.dir, p); err != nil {
		log.Printf("history rotate: %v (logging continues on current segment)", err)
		return
	}
	h, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		// The old handle still appends to the renamed segment; keep using it.
		log.Printf("history reopen: %v (logging continues on rotated segment)", err)
		return
	}
	s.hist, s.histSz = h, 0
	old.Close()
}

func (s *Store) Close() { s.hist.Close() }
