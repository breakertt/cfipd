package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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

func TestV2RestartPreservesValidationProgressAndDeadline(t *testing.T) {
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
