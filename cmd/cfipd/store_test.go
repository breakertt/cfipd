package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestLoadMergesLegacyPrefixKeys(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// v1 sampler wrote four-part keys, v1 recorder wrote three-part keys.
	snap := `{"saved_at":"2026-09-27T00:00:00Z","ips":{},"prefixes":{
		"172.64.52.0":{"prefix":"172.64.52.0","probes":3,"ok":3,"colo_ok":3,"rtt_sum":600,"rtt_count":3,"last_probe":"2026-09-27T00:00:00Z"},
		"172.64.52":{"prefix":"172.64.52","probes":2,"ok":1,"colo_ok":1,"rtt_sum":250,"rtt_count":1,"last_probe":"2026-10-10T00:00:00Z"}}}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(snap), 0o644); err != nil {
		t.Fatal(err)
	}
	d := testDaemon(t)
	if err := st.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	if len(d.scan.stats) != 1 {
		t.Fatalf("expected 1 merged prefix, got %d", len(d.scan.stats))
	}
	m := d.scan.stats["172.64.52"]
	if m == nil || m.Probes != 5 || m.OK != 4 || m.ColoOK != 4 || m.RTTCount != 4 {
		t.Fatalf("merged stat wrong: %+v", m)
	}
	if got := m.LastProbe.Format("2006-01-02"); got != "2026-10-10" {
		t.Fatalf("merged LastProbe = %s, want newest", got)
	}
}

func TestLoadMigratesQuarantine(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenStore(dir)
	defer st.Close()
	snap := `{"ips":{"1.2.3.4":{"addr":"1.2.3.4","role":"quarantine","quar_until":"2030-01-01T00:00:00Z",
		"quar_level":7,"first_seen":"2026-01-01T00:00:00Z","last_seen":"2026-01-01T00:00:00Z"}}}`
	os.WriteFile(filepath.Join(dir, "state.json"), []byte(snap), 0o644)
	d := testDaemon(t)
	before := time.Now()
	if err := st.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	ip := d.pool.get("1.2.3.4")
	if ip == nil || ip.Role != RoleCandidate {
		t.Fatalf("quarantine should migrate to candidate, got %+v", ip)
	}
	if ip.CooldownUntil.After(before.Add(d.cfg.Cooldown.Duration + time.Minute)) {
		t.Fatalf("v1 12h penalty must collapse to one bounded cooldown, got %v", ip.CooldownUntil)
	}
	if !ip.NextProbe.Equal(ip.CooldownUntil) {
		t.Fatalf("NextProbe should be the cooldown end")
	}
}

// TestHistoryRotationPreserves: rotation never overwrites an archive and the
// tracked size starts from the existing file.
func TestHistoryRotationPreserves(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(p, make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.histSz != 1000 {
		t.Fatalf("tracked size must initialize from existing file, got %d", st.histSz)
	}
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("rotation must move the segment aside")
	}
	// archive exists and a second rotation within the same second must not clobber it
	os.WriteFile(p, []byte("x"), 0o644)
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	n := 0
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "history.jsonl" && filepath.Ext(e.Name()) == ".jsonl" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("both archives must be preserved, found %d", n)
	}
}

// TestHistoryRotationPrunesOldest: rotation keeps only the newest histKeep
// archives. The archive files are fixtures, not production history.
func TestHistoryRotationPrunesOldest(t *testing.T) {
	dir := t.TempDir()
	var fixtures []string
	for i := range histKeep + 1 {
		f := filepath.Join(dir, fmt.Sprintf("history-2000010%d-000000.jsonl", i+1))
		if err := os.WriteFile(f, []byte("fixture\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, f)
	}
	p := filepath.Join(dir, "history.jsonl")
	if err := os.WriteFile(p, []byte("fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := rotateFile(dir, p); err != nil {
		t.Fatal(err)
	}
	left, _ := filepath.Glob(filepath.Join(dir, "history-*.jsonl"))
	if len(left) != histKeep {
		t.Fatalf("want %d archives after pruning, found %d", histKeep, len(left))
	}
	for i, f := range fixtures {
		_, err := os.Stat(f)
		if pruned := os.IsNotExist(err); pruned != (i < 2) {
			t.Fatalf("archive %s: pruned=%v, want only the two oldest pruned", filepath.Base(f), pruned)
		}
	}
}

func TestLegacyNonTargetCandidatesDoNotOccupyValidationCapacity(t *testing.T) {
	d := testDaemon(t)
	now := time.Now()
	ip := &IP{Addr: "198.18.0.1", Role: RoleCandidate, Colo: "LAX", FirstSeen: now, LastSeen: now}
	b, err := json.Marshal(snapshot{SavedAt: now, IPs: map[string]*IP{ip.Addr: ip}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.store.dir, "state.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.store.Load(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	if len(d.pool.candidates()) != 0 {
		t.Fatal("legacy wrong-colo candidates must not block new useful admission")
	}
}

func TestRestartPreservesValidationProgressAndDeadline(t *testing.T) {
	d := testDaemon(t)
	now := time.Now()
	ip := &IP{Addr: "198.18.0.1", Role: RoleCandidate, Colo: "NRT", FirstSeen: now.Add(-time.Hour), CycleSince: now.Add(-2 * time.Minute), NextProbe: now.Add(time.Minute), LastSeen: now,
		Win: []Obs{{OK: true, RTT: 180, Colo: "NRT", T: now.Add(-time.Minute)}, {OK: true, RTT: 190, Colo: "NRT", T: now}}}
	d.pool.ips[ip.Addr] = ip
	if err := d.store.Save(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	p := NewPool(d.cfg)
	scan := NewScanner(d.cfg, func(addr string) bool { return p.get(addr) != nil }, p.bestActiveMedian)
	if err := d.store.Load(p, scan); err != nil {
		t.Fatal(err)
	}
	got := p.get(ip.Addr)
	if got == nil || len(got.Win) != 2 || !got.CycleSince.Equal(ip.CycleSince) || !got.NextProbe.Equal(ip.NextProbe) {
		t.Fatal("v2 restart must not discard truthful observations or extend the validation cycle")
	}
}

func TestV1LoadBoundsAndUntrustedStandbyWindow(t *testing.T) {
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

// TestSpeedNextSurvivesRestart: the download gap and the HTTP 429 pause are
// scheduling times and must survive a restart.
func TestSpeedNextSurvivesRestart(t *testing.T) {
	d := testDaemon(t)
	next, paused := time.Now().Add(40*time.Minute), time.Now().Add(70*time.Minute)
	d.pool.speedNext, d.pool.speedPaused = next, paused
	if err := d.store.Save(d.pool, d.scan); err != nil {
		t.Fatal(err)
	}
	p := NewPool(d.cfg)
	scan := NewScanner(d.cfg, func(string) bool { return false }, p.bestActiveMedian)
	if err := d.store.Load(p, scan); err != nil {
		t.Fatal(err)
	}
	if !p.speedNext.Equal(next) || !p.speedPaused.Equal(paused) {
		t.Fatalf("download schedule lost across restart: got %v/%v want %v/%v", p.speedNext, p.speedPaused, next, paused)
	}
}

// snapshotFixture is a small snapshot with every field set. Fixture data, not
// production state.
func snapshotFixture() *snapshot {
	at := time.Date(2026, 10, 20, 8, 0, 0, 0, time.UTC)
	return &snapshot{
		SavedAt: at,
		IPs: map[string]*IP{
			"198.18.0.1": {Addr: "198.18.0.1", Role: RoleActive, State: StateHealthy, Colo: "SIN", Gen: 3,
				Win: []Obs{{OK: true, RTT: 270, Colo: "SIN", T: at}}, Speed: 80, SpeedAt: at, SpeedDue: at.Add(time.Hour)},
			"198.18.1.1": {Addr: "198.18.1.1", Role: RoleStandby, Colo: "NRT", Gen: 1},
		},
		Prefixes: map[string]*PrefixStat{
			"198.18.0": {Prefix: "198.18.0", Probes: 5, OK: 4, ColoOK: 4, RTTSum: 1000, RTTCount: 4, LastProbe: at},
			"198.18.1": {Prefix: "198.18.1", Probes: 1, LastProbe: at},
		},
		PK:          &pkPersist{ObserveUntil: at, Day: "2026-10-20", BWUsed: 1},
		Day:         "2026-10-20",
		DayCounts:   map[string]int{"health": 7},
		DayBytes:    123,
		SpeedNext:   at.Add(15 * time.Minute),
		SpeedPaused: at.Add(time.Hour),
	}
}

// TestSnapshotStreamMatchesStructFormat: the streamed snapshot decodes, with
// the struct's JSON tags, to exactly what was written, so Load and older
// readers see the same state file format.
func TestSnapshotStreamMatchesStructFormat(t *testing.T) {
	want := snapshotFixture()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, want); err != nil {
		t.Fatal(err)
	}
	var got snapshot
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&got, want) {
		t.Fatalf("streamed snapshot differs under json.Unmarshal:\n got %+v\nwant %+v", got, *want)
	}
}

// TestSnapshotSaveStaysLean: a save streams entry by entry instead of
// building the whole document; it must allocate well under a one-shot
// json.Marshal of the same state, measured in the same process (race mode
// inflates both). The 6000 prefixes are fixtures sized like a production
// state.
func TestSnapshotSaveStaysLean(t *testing.T) {
	d := testDaemon(t)
	for i := range 6000 {
		key := fmt.Sprintf("%d.%d.%d", 100+i/65536, i/256%256, i%256)
		d.scan.stats[key] = &PrefixStat{Prefix: key, Probes: 3, OK: 2, ColoOK: 2, RTTSum: 500, RTTCount: 2, LastProbe: time.Now()}
	}
	alloc := func(f func() error) float64 {
		runtime.GC()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		if err := f(); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&b)
		return float64(b.TotalAlloc-a.TotalAlloc) / 1e6
	}
	oneShot := alloc(func() error { _, err := json.Marshal(&snapshot{IPs: d.pool.ips, Prefixes: d.scan.stats}); return err })
	save := alloc(func() error { return d.store.Save(d.pool, d.scan) })
	t.Logf("save %.2f MB, one-shot json.Marshal %.2f MB", save, oneShot)
	if save > oneShot/2 {
		t.Fatalf("a save allocated %.2f MB vs %.2f MB one-shot; it must stream", save, oneShot)
	}
	p := NewPool(d.cfg)
	scan := NewScanner(d.cfg, func(string) bool { return false }, p.bestActiveMedian)
	if err := d.store.Load(p, scan); err != nil || len(scan.stats) != 6000 {
		t.Fatalf("loaded %d prefixes (%v), want 6000", len(scan.stats), err)
	}
}
