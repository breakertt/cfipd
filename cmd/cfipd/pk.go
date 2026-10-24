package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"
)

// pkRun holds one paired PK comparison: interleaved fresh probes of the
// challenger and the incumbent, taken in the same time window.
type pkRun struct {
	challRTT, incRTT   []float64
	challOK, incOK     int
	challColo, incColo string
}

// pass applies the v2 win contract with the same configurable sampling rules
// as promotion: the challenger passes availability first (PromoteProbes minus
// PromoteMaxFails good samples), then must show a meaningful median
// improvement (>=20% and >=30ms) with p90 not worse. A failed incumbent probe
// counts as slower than any success, so a lossy incumbent loses the
// comparison instead of voiding it; with half or more failed, availability
// alone decides. pkPairEligible has already required the challenger's window
// median to beat the incumbent's, so this cannot crown a slow challenger.
func (r *pkRun) pass(cfg *Config) (bool, string) {
	required := cfg.PromoteProbes - cfg.PromoteMaxFails
	if r.challOK < required {
		return false, fmt.Sprintf("challenger availability %d/%d ok (need %d)", r.challOK, cfg.PromoteProbes, required)
	}
	inc := append([]float64(nil), r.incRTT...)
	for range cfg.PromoteProbes - r.incOK {
		inc = append(inc, math.Inf(1))
	}
	challMed, incMed, challP90, incP90 := median(r.challRTT), median(inc), pct90(r.challRTT), pct90(inc)
	show := func(v float64) string {
		if math.IsInf(v, 1) {
			return "failed"
		}
		return fmt.Sprintf("%.0fms", v)
	}
	if challMed > incMed-30 || challMed > 0.8*incMed {
		return false, fmt.Sprintf("median %.0fms vs %s: improvement < 30ms/20%%", challMed, show(incMed))
	}
	if challP90 > incP90 {
		return false, fmt.Sprintf("p90 %.0fms > incumbent %s", challP90, show(incP90))
	}
	return true, fmt.Sprintf("median %s -> %.0fms, p90 %s -> %.0fms, incumbent %d/%d ok",
		show(incMed), challMed, show(incP90), challP90, r.incOK, cfg.PromoteProbes)
}

type pkPending struct {
	Challenger, Incumbent string
	At                    time.Time
}

// evalPK looks for a credible challenger. Elective comparisons are capped at
// one per 30m; a pending confirmation round follows its own due time. Failed
// pairs back off instead of retriggering on unchanged evidence.
func (d *daemon) evalPK(now time.Time) {
	if d.longJobBlocked(now) != "" {
		return
	}
	sb, worst := d.nextPKPair(now)
	if sb == nil {
		return
	}
	key := sb.Addr + ">" + worst.Addr
	if until, failed := d.pkFailUntil[key]; failed && now.Before(until) {
		return
	}
	d.pkLastEval = now
	d.pkRunning = true
	j := job{kind: jPK, ip: sb.Addr, peer: worst.Addr, gen: sb.Gen, peerGen: worst.Gen}
	log.Printf("PK probe: %s (med=%.0fms) vs %s (med=%.0fms)", sb.Addr, sb.medianRTT(), worst.Addr, worst.medianRTT())
	go func() { d.sendResult(d.execPK(d.ctx, j)) }()
}

// nextPKPair applies the elective-comparison and confirmation deadlines.
// Only the original pending pair can use the earlier confirmation deadline.
func (d *daemon) nextPKPair(now time.Time) (*IP, *IP) {
	if pending := d.pkPending; pending != nil && now.Sub(pending.At) >= 5*time.Minute {
		sb, inc := d.pool.get(pending.Challenger), d.pool.get(pending.Incumbent)
		if d.pool.pkPairEligible(sb, inc, now) {
			return sb, inc
		}
		d.pkPending = nil
	}
	if now.Sub(d.pkLastEval) < 30*time.Minute {
		return nil, nil
	}
	return d.pool.pickPKPair(now)
}

func usablePKIncumbent(a *IP) bool {
	return a != nil && a.Role == RoleActive && a.medianRTT() > 0 && len(a.Win) > 0 &&
		(a.State == StateHealthy || a.Win[len(a.Win)-1].OK)
}

// pkPairEligible is shared by new pair selection and pending confirmations.
func (p *Pool) pkPairEligible(sb, inc *IP, now time.Time) bool {
	if !usablePKIncumbent(inc) || sb == nil || sb.Role != RoleStandby || sb.Promoting ||
		now.Sub(sb.LastSeen) > 10*time.Minute || len(sb.okRTTs()) < 4 {
		return false
	}
	med, reference := sb.medianRTT(), inc.medianRTT()
	if med > reference-30 || med > 0.8*reference {
		return false
	}
	return p.swapDiverse(sb, inc)
}

// pickPKPair targets the worst usable active with a fresh credible challenger.
// Degraded-but-available incumbents belong here; unavailable ones need fill.
func (p *Pool) pickPKPair(now time.Time) (challenger, incumbent *IP) {
	for _, a := range p.actives() {
		if usablePKIncumbent(a) && (incumbent == nil || a.medianRTT() > incumbent.medianRTT()) {
			incumbent = a
		}
	}
	if incumbent == nil {
		return nil, nil
	}
	for _, sb := range p.rankedStandbys() {
		if p.pkPairEligible(sb, incumbent, now) {
			return sb, incumbent
		}
	}
	return nil, nil
}

// execPK probes challenger and incumbent interleaved, PromoteProbes rounds,
// keeping every physical result.
func (d *daemon) execPK(ctx context.Context, j job) jobResult {
	cfg := d.cfg
	res := jobResult{job: j, pk: &pkRun{}}
	for i := 0; i < cfg.PromoteProbes; i++ {
		for _, leg := range []struct {
			addr   string
			dst    *[]float64
			ok     *int
			colo   *string
			physic *[]ProbeResult
		}{{j.ip, &res.pk.challRTT, &res.pk.challOK, &res.pk.challColo, &res.probes},
			{j.peer, &res.pk.incRTT, &res.pk.incOK, &res.pk.incColo, &res.peerProbes}} {
			r := probe(ctx, cfg, leg.addr)
			*leg.physic = append(*leg.physic, r)
			if r.OK && cfg.coloAllowed(r.Colo) {
				*leg.dst = append(*leg.dst, r.Totalms)
				*leg.ok++
				*leg.colo = r.Colo
			}
		}
		if i+1 < cfg.PromoteProbes {
			select {
			case <-ctx.Done():
				res.err = "cancelled"
				return res
			case <-time.After(cfg.PromoteInterval.Duration):
			}
		}
	}
	res.ok = true
	return res
}

// execSpeed measures challenger then incumbent serially in the same window.
// Only ever dispatched for an actual PK winner, bounded by the daily budget.
func (d *daemon) execSpeed(ctx context.Context, j job) jobResult {
	res := jobResult{job: j}
	res.speed = speedTest(ctx, d.cfg, j.ip)
	select {
	case <-ctx.Done():
		res.err = "cancelled"
		return res
	default:
	}
	res.speed2 = speedTest(ctx, d.cfg, j.peer)
	res.ok = true
	return res
}

// applyPK scores a paired probe round; a first pass arms a pending pair, a
// second independent pass (>=5min later) confirms and moves to bandwidth.
// An early second pass does not reset the first win's age.
func (d *daemon) applyPK(r jobResult, now time.Time) {
	if r.err == "cancelled" || r.pk == nil {
		return
	}
	sb, inc := d.pool.get(r.ip), d.pool.get(r.peer)
	if sb == nil || inc == nil || sb.Role != RoleStandby || inc.Role != RoleActive {
		return
	}
	pass, why := r.pk.pass(d.cfg)
	d.pkLastVerdict = fmt.Sprintf("%s vs %s: %s", r.ip, r.peer, why)
	d.store.Log(map[string]any{"type": "pk", "challenger": r.ip, "incumbent": r.peer,
		"pass": pass, "why": why, "t": now})
	if !pass {
		d.pkFailUntil[r.ip+">"+r.peer] = now.Add(30 * time.Minute)
		if d.pkPending != nil && d.pkPending.Challenger == r.ip {
			d.pkPending = nil
		}
		log.Printf("PK %s vs %s: no win (%s)", r.ip, r.peer, why)
		return
	}
	if d.pkPending != nil && d.pkPending.Challenger == r.ip && d.pkPending.Incumbent == r.peer {
		if now.Sub(d.pkPending.At) < 5*time.Minute {
			log.Printf("PK %s vs %s: win too early to confirm, keeping first-win age", r.ip, r.peer)
			return
		}
	} else {
		d.pkPending = &pkPending{Challenger: r.ip, Incumbent: r.peer, At: now}
		log.Printf("PK %s vs %s: first win (%s), awaiting confirmation", r.ip, r.peer, why)
		return
	}
	d.pkPending = nil
	// Confirmed winner: spend from the daily full-comparison budget.
	day := now.Format("2006-01-02")
	if d.pool.pk.Day != day {
		d.pool.pk.Day, d.pool.pk.BWUsed = day, 0
	}
	if d.pool.pk.BWUsed >= 2 {
		log.Printf("PK %s confirmed (%s) but daily bandwidth budget spent; light observation continues", r.ip, why)
		return
	}
	d.pool.pk.BWUsed++
	d.pkRunning = true
	d.pkLastRun = r.pk // fresh paired evidence carries into the bandwidth stage
	// No longJobBlocked check here: evalPK passed it when this PK started, and
	// pkRunning has blocked every other long job and download since then.
	j := job{kind: jSpeed, ip: r.ip, peer: r.peer, gen: sb.Gen, peerGen: inc.Gen}
	log.Printf("PK %s confirmed (%s), bandwidth check (%d/2 today)", r.ip, why, d.pool.pk.BWUsed)
	go func() { d.sendResult(d.execSpeed(d.ctx, j)) }()
}

// applySpeed takes the final PK decision. Both serial measurements must be
// valid with acceptable colos — a missing incumbent reference is inconclusive,
// never a win. Roles and diversity are revalidated at commit time; the swap is
// one-for-one, so a short pool does not block it.
// The 80%-of-incumbent guard is a provisional starting heuristic, to be
// calibrated from observed PK rounds.
func (d *daemon) applySpeed(r jobResult, now time.Time) {
	d.pkRunning = false
	if r.err == "cancelled" {
		return
	}
	sb, inc := d.pool.get(r.ip), d.pool.get(r.peer)
	if sb == nil || inc == nil || sb.Role != RoleStandby || inc.Role != RoleActive {
		return
	}
	// Valid samples are recorded whatever the verdict: they are real
	// measurements of both endpoints.
	if d.validSpeed(r.speed) {
		d.pool.recordSpeed(sb, r.speed.Mbps, now)
	}
	if d.validSpeed(r.speed2) {
		d.pool.recordSpeed(inc, r.speed2.Mbps, now)
	}
	win := d.validSpeed(r.speed) && d.validSpeed(r.speed2) &&
		r.speed.Mbps >= d.cfg.PromoteMinMbps && r.speed2.Mbps > 0 &&
		r.speed.Mbps >= 0.8*r.speed2.Mbps &&
		d.pool.swapDiverse(sb, inc)
	d.store.Log(map[string]any{"type": "pk_speed", "challenger": r.ip, "incumbent": r.peer,
		"chall_mbps": r.speed.Mbps, "inc_mbps": r.speed2.Mbps, "chall_err": r.speed.Err,
		"inc_err": r.speed2.Err, "win": win, "t": now})
	if !win {
		// A real bandwidth loss holds for a day; without it the same
		// low-latency challenger would win every latency round again. An
		// inconclusive stage (a failed download) holds only for an hour.
		hold := time.Hour
		if d.validSpeed(r.speed) && d.validSpeed(r.speed2) {
			hold = 24 * time.Hour
		}
		d.pkFailUntil[r.ip+">"+r.peer] = now.Add(hold)
		d.pkLastVerdict = fmt.Sprintf("%s vs %s: bandwidth %.1f vs %.1f Mbps, no swap", r.ip, r.peer, r.speed.Mbps, r.speed2.Mbps)
		log.Printf("PK %s bandwidth insufficient or inconclusive: %.1fMbps (%s) vs incumbent %.1fMbps (%s)",
			r.ip, r.speed.Mbps, r.speed.Err, r.speed2.Mbps, r.speed2.Err)
		return
	}
	if now.Before(d.pool.pk.ObserveUntil) {
		d.pkLastVerdict = fmt.Sprintf("%s -> %s would swap (%.1f vs %.1f Mbps), observing", r.peer, r.ip, r.speed.Mbps, r.speed2.Mbps)
		log.Printf("PK would swap %s -> %s (%.1f vs %.1f Mbps); observing until %s",
			r.peer, r.ip, r.speed.Mbps, r.speed2.Mbps, d.pool.pk.ObserveUntil.Format(time.RFC3339))
		return
	}
	// Decide on the fresh paired medians/colos, not the old standby window.
	med, colo := sb.medianRTT(), sb.Colo
	if d.pkLastRun != nil {
		if m := median(d.pkLastRun.challRTT); m > 0 {
			med = m
		}
		if d.pkLastRun.challColo != "" {
			colo = d.pkLastRun.challColo
		}
	}
	sb.Colo = colo
	d.pool.swapPK(sb, inc, med, now)
	d.pkLastVerdict = fmt.Sprintf("%s -> %s swapped (%.1f vs %.1f Mbps)", r.peer, r.ip, r.speed.Mbps, r.speed2.Mbps)
	d.logEvent(fmt.Sprintf("PK swap: %s -> active, %s -> standby (%.1f vs %.1f Mbps)",
		sb.Addr, inc.Addr, r.speed.Mbps, r.speed2.Mbps), now)
}
