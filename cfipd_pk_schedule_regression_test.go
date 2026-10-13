package main

import (
	"testing"
	"time"
)

func TestPKConfirmationKeepsOriginalPairWhenRankingChanges(t *testing.T) {
	d, inc, sb := regressionPair(t)
	d.pkPending = &pkPending{Challenger: sb.Addr, Incumbent: inc.Addr, At: t0}
	d.pkLastEval = t0
	better := &IP{Addr: "198.18.4.1", Role: RoleStandby, Colo: "KIX", LastSeen: t0, Win: append([]Obs(nil), sb.Win...)}
	for i := range better.Win {
		better.Win[i].RTT = 100
	}
	d.pool.ips[better.Addr] = better
	// A different active is now worst, but the original pair still qualifies.
	for i := range d.pool.ips["198.18.1.1"].Win {
		d.pool.ips["198.18.1.1"].Win[i].RTT = 500
	}
	gotSB, gotInc := d.nextPKPair(t0.Add(6 * time.Minute))
	if gotSB != sb || gotInc != inc {
		t.Fatalf("confirmation must repeat its original pair, got (%v, %v)", gotSB, gotInc)
	}
}

func TestInvalidPKPendingPairDoesNotBypassElectiveLimit(t *testing.T) {
	d, inc, sb := regressionPair(t)
	d.pkPending = &pkPending{Challenger: "198.18.9.1", Incumbent: inc.Addr, At: t0}
	d.pkLastEval = t0
	if challenger, _ := d.nextPKPair(t0.Add(6 * time.Minute)); challenger != nil {
		t.Fatal("expired confirmation must not authorize a new pair before 30min")
	}
	if d.pkPending != nil {
		t.Fatal("invalid pending pair must be cleared")
	}
	// Once the elective deadline arrives, fresh evidence can start a new pair.
	sb.LastSeen = t0.Add(30 * time.Minute)
	if challenger, incumbent := d.nextPKPair(t0.Add(30 * time.Minute)); challenger != sb || incumbent != inc {
		t.Fatal("new pairs must remain possible at the ordinary elective deadline")
	}
}
