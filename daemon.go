package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type jobKind int

const (
	jHealth jobKind = iota
	jStandby
	jValidate
	jDiscover
	jPromote
	jPK
	jSpeed
	jRelay
)

func (k jobKind) String() string {
	return [...]string{"health", "standby", "validate", "discover", "promote", "pk", "speed", "relay"}[k]
}

// longJob reports whether the job is scoped to the pool generation (regular
// probes are scoped to their IP's own generation instead).
func (k jobKind) longJob() bool { return k == jPromote || k == jPK || k == jSpeed }

type job struct {
	kind  jobKind
	ip    string
	peer  string  // PK incumbent / speed comparison partner
	limit float64 // median RTT ceiling snapshotted at dispatch
	gen   uint64  // dispatch-time generation evidence (pool gen for long jobs, IP gen otherwise)
}

type jobResult struct {
	job
	probe ProbeResult
	speed SpeedResult
	med   float64
	jit   float64
	fails int
	ok    bool
	err   string

	// probes/peerProbes are the truthful per-request results of promote/PK
	// jobs, with real timestamps; never synthesized from a summary.
	probes     []ProbeResult
	peerProbes []ProbeResult
	pk         *pkRun
	speed2     SpeedResult // jSpeed: incumbent measured in the same window
}

// pkRun holds one paired PK comparison: interleaved fresh probes of the
// challenger and the incumbent, taken in the same time window.
type pkRun struct {
	challRTT, incRTT   []float64
	challOK, incOK     int
	challColo, incColo string
}

func (r *pkRun) summary() (challMed, incMed, challP90, incP90 float64) {
	return median(r.challRTT), median(r.incRTT), pct90(r.challRTT), pct90(r.incRTT)
}

// pass applies the v2 win contract with the same configurable sampling rules
// as promotion: availability first (PromoteProbes minus PromoteMaxFails good
// samples each side), then meaningful median improvement (>=20% and >=30ms)
// with p90 not worse.
func (r *pkRun) pass(cfg *Config) (bool, string) {
	required := cfg.PromoteProbes - cfg.PromoteMaxFails
	if r.challOK < required || r.incOK < required {
		return false, fmt.Sprintf("availability %d/%d ok of %d (need %d)", r.challOK, r.incOK, cfg.PromoteProbes, required)
	}
	challMed, incMed, challP90, incP90 := r.summary()
	if challMed > incMed-30 || challMed > 0.8*incMed {
		return false, fmt.Sprintf("median %.0fms vs %.0fms: improvement < 30ms/20%%", challMed, incMed)
	}
	if challP90 > incP90 {
		return false, fmt.Sprintf("p90 %.0fms > incumbent %.0fms", challP90, incP90)
	}
	return true, fmt.Sprintf("median %.0fms -> %.0fms, p90 %.0fms -> %.0fms", incMed, challMed, incP90, challP90)
}

type pkPending struct {
	Challenger, Incumbent string
	At                    time.Time
}

type daemon struct {
	cfg   *Config
	pool  *Pool
	scan  *Scanner
	store *Store

	jobs     chan job
	results  chan jobResult
	inflight map[string]bool
	ctl      chan ctlRequest

	promoting    bool
	nextScan     time.Time
	lastValidate time.Time
	started      time.Time
	rnd          *rand.Rand

	pkRunning     bool
	pkPending     *pkPending
	pkLastEval    time.Time
	pkFailUntil   map[string]time.Time // "challenger>incumbent" backoff for unchanged evidence
	pkLastRun     *pkRun               // confirmed round carried into the bandwidth stage
	pkLastVerdict string

	promoteFail map[string]time.Time // bound mandatory-promotion retries

	relaySnap   atomic.Value // []relayTarget, written by event loop, read by relay goroutines
	relayStats  map[string]*RelayStat
	ctx         context.Context
	discovering int

	hourStart  time.Time
	hourCounts map[string]int
	dayCounts  map[string]int // mirrors pool.dayCounts (same map header)
}

func newDaemon(cfg *Config, store *Store) *daemon {
	d := &daemon{
		cfg:         cfg,
		store:       store,
		jobs:        make(chan job, 64),
		results:     make(chan jobResult, 64),
		inflight:    map[string]bool{},
		ctl:         make(chan ctlRequest),
		started:     time.Now(),
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
		relayStats:  map[string]*RelayStat{},
		pkFailUntil: map[string]time.Time{},
		promoteFail: map[string]time.Time{},
		hourCounts:  map[string]int{},
		hourStart:   time.Now().Truncate(time.Hour),
	}
	d.pool = NewPool(cfg)
	// Default before Load: a fresh install also observes PK for a day.
	d.pool.pk = &pkPersist{ObserveUntil: time.Now().Add(24 * time.Hour)}
	d.scan = NewScanner(cfg,
		func(addr string) bool { return d.pool.get(addr) != nil },
		func() float64 { return d.pool.bestActiveMedian() })
	d.dayCounts = d.pool.dayCounts
	return d
}

func (d *daemon) run(ctx context.Context) error {
	d.ctx = ctx
	for range d.cfg.Workers {
		go d.worker(ctx)
	}
	if err := serveCtl(d); err != nil {
		return err
	}
	d.updateRelaySnap()
	d.dayCounts = d.pool.dayCounts // Load may have restored persisted counts
	go d.serveRelay(ctx)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	snap := time.NewTicker(5 * time.Minute)
	defer snap.Stop()
	log.Printf("cfipd started, data=%s socket=%s", d.cfg.DataDir, d.cfg.Socket)
	for {
		select {
		case <-ctx.Done():
			if err := d.store.Save(d.pool, d.scan); err != nil {
				log.Printf("final save: %v", err)
			}
			return nil
		case now := <-tick.C:
			d.schedule(now)
		case r := <-d.results:
			d.apply(r)
		case req := <-d.ctl:
			d.handleCtl(req)
		case <-snap.C:
			d.pool.pruneCandidates(time.Now())
			if err := d.store.Save(d.pool, d.scan); err != nil {
				log.Printf("save: %v", err)
			}
		}
	}
}

func (d *daemon) worker(ctx context.Context) {
	for {
		var j job
		select {
		case <-ctx.Done():
			return
		case next, ok := <-d.jobs:
			if !ok {
				return
			}
			j = next
		}
		select {
		case d.results <- d.exec(ctx, j):
		case <-ctx.Done():
			return // never block a worker on a result nobody will read
		}
	}
}

// sendResult delivers from a dedicated long-job goroutine without leaking it
// after shutdown.
func (d *daemon) sendResult(r jobResult) {
	select {
	case d.results <- r:
	case <-d.ctx.Done():
	}
}

// exec runs probe work with immutable inputs only. Workers never touch the
// pool; the RTT limit and generation are snapshotted at dispatch.
func (d *daemon) exec(ctx context.Context, j job) jobResult {
	switch j.kind {
	case jPromote:
		return d.execPromote(ctx, j)
	case jPK:
		return d.execPK(ctx, j)
	case jSpeed:
		return d.execSpeed(ctx, j)
	default:
		return jobResult{job: j, probe: probe(ctx, d.cfg, j.ip)}
	}
}

func (d *daemon) execPromote(ctx context.Context, j job) jobResult {
	cfg := d.cfg
	res := jobResult{job: j}
	var rtts []float64
	var colo string
	for i := 0; i < cfg.PromoteProbes; i++ {
		r := probe(ctx, cfg, j.ip)
		res.probes = append(res.probes, r) // truthful per-request record
		if r.OK && cfg.coloAllowed(r.Colo) {
			rtts = append(rtts, r.Totalms)
			colo = r.Colo
		} else {
			res.fails++
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
	res.med, res.jit = median(rtts), 0.0
	if len(rtts) >= 2 {
		res.jit = pct90(rtts) - res.med
	}
	if res.fails > cfg.PromoteMaxFails {
		res.err = fmt.Sprintf("%d/%d probes failed", res.fails, cfg.PromoteProbes)
		return res
	}
	if len(rtts) == 0 {
		res.err = "no successful probes"
		return res
	}
	if res.med > j.limit {
		res.err = fmt.Sprintf("median %.0fms > %.0fms", res.med, j.limit)
		return res
	}
	if res.jit > cfg.PromoteMaxJitter {
		res.err = fmt.Sprintf("jitter %.0fms > %.0fms", res.jit, cfg.PromoteMaxJitter)
		return res
	}
	sp := speedTest(ctx, cfg, j.ip)
	res.speed = sp
	if sp.Mbps < cfg.PromoteMinMbps {
		res.err = fmt.Sprintf("speed %.1fMbps < %.1fMbps (%s)", sp.Mbps, cfg.PromoteMinMbps, sp.Err)
		return res
	}
	if sp.Colo != "" && !cfg.coloAllowed(sp.Colo) {
		res.err = fmt.Sprintf("colo flipped to %s after speed test", sp.Colo)
		return res
	}
	res.ok = true
	res.probe = ProbeResult{OK: true, Colo: colo, Totalms: res.med}
	return res
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

func (d *daemon) enqueue(j job) bool {
	key := j.kind.String() + j.ip
	if d.inflight[key] {
		return false
	}
	select {
	case d.jobs <- j:
		d.inflight[key] = true
		return true
	default:
		log.Printf("job queue full, dropping %s %s", j.kind, j.ip)
		return false
	}
}

func (d *daemon) schedule(now time.Time) {
	cfg := d.cfg

	for _, a := range d.pool.actives() {
		if now.Before(a.NextProbe) {
			continue
		}
		// Advance the deadline only when the job is actually dispatched.
		if d.enqueue(job{kind: jHealth, ip: a.Addr, gen: a.Gen}) {
			a.NextProbe = now.Add(d.pool.jitterDown(a.healthEvery(cfg)))
		}
	}

	// Standbys are per-IP scheduled by their own NextProbe, most overdue
	// first; no round-robin over a randomly-ordered map.
	sbs := d.pool.standbys()
	sortByNextProbe(sbs)
	enq := 0
	for _, sb := range sbs {
		if enq >= 2 {
			break
		}
		if !now.Before(sb.NextProbe) && !sb.Promoting {
			if d.enqueue(job{kind: jStandby, ip: sb.Addr, gen: sb.Gen}) {
				sb.NextProbe = now.Add(d.pool.jitterDown(d.pool.standbyEvery(sb)))
				enq++
			}
		}
	}

	// Candidates validate only with a purpose: backup gap / unhealthy active,
	// or this candidate itself is a credible improvement. Rate is a cap of
	// one dispatch per 5s, not a quota to spend.
	if now.Sub(d.lastValidate) >= 5*time.Second {
		deficit := len(d.pool.actives()) < cfg.ActiveCount || len(d.pool.standbys()) < cfg.StandbyTarget
		if !deficit {
			for _, a := range d.pool.actives() {
				if a.State != StateHealthy {
					deficit = true
					break
				}
			}
		}
		cands := d.pool.candidates()
		sortByNextProbe(cands)
		for _, c := range cands {
			if now.Before(c.NextProbe) || c.CooldownUntil.After(now) {
				continue
			}
			if !deficit && !d.pool.credibleImprovement(c) {
				continue // a promising candidate never makes unrelated ones busy
			}
			if d.enqueue(job{kind: jValidate, ip: c.Addr, gen: c.Gen}) {
				d.lastValidate = now
				break
			}
		}
	}

	if !now.Before(d.nextScan) {
		d.nextScan = now.Add(d.pool.jitterDown(d.scanInterval()))
		if d.discovering < 8 {
			if ip := d.scan.pick(); ip != "" && d.enqueue(job{kind: jDiscover, ip: ip}) {
				d.discovering++
			}
		}
	}

	// Hard-failure fill has priority over any elective PK.
	if !d.promoting && !d.pkRunning && len(d.pool.actives()) < cfg.ActiveCount {
		d.fillActive(now)
	}

	d.evalPK(now)
	d.rollHour(now)
	d.updateRelaySnap()
}

func sortByNextProbe(ips []*IP) {
	for i := 1; i < len(ips); i++ {
		for j := i; j > 0 && ips[j].NextProbe.Before(ips[j-1].NextProbe); j-- {
			ips[j], ips[j-1] = ips[j-1], ips[j]
		}
	}
}

// fillActive tries standbys best-first; a recently failed promotion backs off
// instead of re-running a 99MB test every tick.
func (d *daemon) fillActive(now time.Time) {
	for _, sb := range d.pool.rankedStandbys() {
		if sb.Promoting {
			continue
		}
		if t, failed := d.promoteFail[sb.Addr]; failed && now.Sub(t) < 10*time.Minute {
			continue
		}
		if !d.pool.diverseWith(sb, d.pool.actives()) {
			continue
		}
		if now.Sub(sb.LastSeen) > 2*time.Minute {
			if d.enqueue(job{kind: jStandby, ip: sb.Addr, gen: sb.Gen}) {
				sb.NextProbe = now.Add(d.pool.jitterDown(d.pool.standbyEvery(sb)))
			}
			return // refresh the stale backup before trusting it
		}
		d.startPromotion(sb)
		return
	}
}

// startPromotion runs the promotion test on a dedicated goroutine: it must
// never wait in the shared job queue behind discovery floods.
func (d *daemon) startPromotion(sb *IP) {
	sb.Promoting = true
	d.promoting = true
	j := job{kind: jPromote, ip: sb.Addr, limit: d.pool.rttLimit(), gen: d.pool.gen}
	log.Printf("promotion test: %s (colo=%s med=%.0fms)", sb.Addr, sb.Colo, sb.medianRTT())
	go func() { d.sendResult(d.execPromote(d.ctx, j)) }()
}

// scanInterval follows need and useful yield: bootstrap fast, deficit fast,
// stable backs off from ScanStable toward ScanMax after 20 yield-free scans.
func (d *daemon) scanInterval() time.Duration {
	cfg := d.cfg
	nSb, nAc := len(d.pool.standbys()), len(d.pool.actives())
	switch {
	case nSb == 0 && nAc == 0:
		return cfg.ScanBootstrap.Duration
	case nSb < cfg.StandbyMin || nAc < cfg.ActiveCount:
		return cfg.ScanLow.Duration
	}
	dur := cfg.ScanStable.Duration << min(d.scan.noYield/20, 10)
	if dur > cfg.ScanMax.Duration {
		dur = cfg.ScanMax.Duration
	}
	return dur
}

// evalPK looks for a credible challenger. Elective comparisons are capped at
// one per 30m; a pending confirmation round follows its own due time. Failed
// pairs back off instead of retriggering on unchanged evidence.
func (d *daemon) evalPK(now time.Time) {
	cfg := d.cfg
	if d.promoting || d.pkRunning || len(d.pool.actives()) != cfg.ActiveCount {
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
	j := job{kind: jPK, ip: sb.Addr, peer: worst.Addr, gen: d.pool.gen}
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
	others := make([]*IP, 0, p.cfg.ActiveCount-1)
	for _, a := range p.actives() {
		if a != inc {
			others = append(others, a)
		}
	}
	return p.diverseWith(sb, others)
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

// credibleImprovement reports whether this candidate alone looks like a
// meaningful win over the worst active.
func (p *Pool) credibleImprovement(c *IP) bool {
	worst := 0.0
	for _, a := range p.actives() {
		if m := a.medianRTT(); m > worst {
			worst = m
		}
	}
	if worst <= 0 {
		return false
	}
	m := c.medianRTT()
	return m > 0 && m <= worst-30 && m <= 0.8*worst
}

func (d *daemon) apply(r jobResult) {
	if r.job.kind != jRelay {
		delete(d.inflight, r.job.kind.String()+r.ip)
	}
	if r.job.kind == jDiscover {
		d.discovering--
	}
	now := time.Now()
	ip := d.pool.get(r.ip)

	// Stale evidence is dropped: long jobs are scoped to the pool generation,
	// regular probes to their IP's own generation. Generation zero is a real
	// captured generation, not an opt-out.
	stale := false
	if r.job.kind.longJob() {
		stale = r.job.gen != d.pool.gen
	} else if ip != nil && r.job.kind != jDiscover {
		stale = r.job.gen != ip.Gen
	}
	// Physical work is counted and fed to prefix learning regardless of the
	// decision outcome or staleness — the requests really happened.
	// Participant windows consume results only when the evidence is current.
	participant := ip
	if stale {
		participant = nil
	}
	d.countPhysical(r, now, participant)
	if stale {
		if ip != nil && r.job.kind == jPromote {
			ip.Promoting = false
			d.promoting = false
		}
		if r.job.kind == jPK || r.job.kind == jSpeed {
			d.pkRunning = false
		}
		return
	}

	switch r.job.kind {
	case jHealth:
		if ip == nil || ip.Role != RoleActive {
			return
		}
		for _, ev := range d.pool.applyHealth(ip, r.probe, now) {
			d.logEvent(ev, now)
		}
	case jStandby:
		if ip == nil || ip.Role != RoleStandby {
			return
		}
		for _, ev := range d.pool.applyStandby(ip, r.probe, now) {
			d.logEvent(ev, now)
		}
	case jDiscover:
		d.scan.Record(r.ip, r.probe, now)
		useful := false
		prospect := &IP{Addr: r.ip, Colo: r.probe.Colo,
			Win: []Obs{{OK: r.probe.OK, RTT: r.probe.Totalms, Colo: r.probe.Colo, T: r.probe.T}}}
		deficit := len(d.pool.actives()) < d.cfg.ActiveCount || len(d.pool.standbys()) < d.cfg.StandbyTarget
		if r.probe.OK && d.cfg.coloAllowed(r.probe.Colo) && ip == nil &&
			(deficit || d.pool.credibleImprovement(prospect)) && d.candidateRoom(r.ip) {
			// Useful yield = new candidate work serving a current deficit or
			// improvement; a right-colo probe rejected by capacity or lack of
			// need is prefix learning only.
			ip = d.pool.ensure(r.ip, now)
			useful = true
		}
		if useful {
			d.scan.noYield = 0
		} else {
			d.scan.noYield++
		}
		if ip != nil && ip.Role == RoleCandidate {
			for _, ev := range d.pool.applyCandidate(ip, r.probe, now) {
				d.logEvent(ev, now)
			}
		}
	case jValidate:
		if ip == nil || ip.Role != RoleCandidate {
			return
		}
		d.scan.Record(r.ip, r.probe, now)
		for _, ev := range d.pool.applyCandidate(ip, r.probe, now) {
			d.logEvent(ev, now)
		}
	case jPromote:
		d.promoting = false
		if ip == nil {
			return
		}
		ip.Promoting = false
		if r.err == "cancelled" {
			return // neutral: no punishment for a cancelled test
		}
		if r.ok && ip.Role == RoleStandby && len(d.pool.actives()) < d.cfg.ActiveCount &&
			d.pool.diverseWith(ip, d.pool.actives()) {
			delete(d.promoteFail, ip.Addr)
			ip.Speed, ip.SpeedAt = r.speed.Mbps, now
			d.pool.promoteActive(ip, r.med, now)
			d.logEvent(fmt.Sprintf("%s promoted -> active (colo=%s med=%.0fms jit=%.0fms speed=%.1fMbps)",
				ip.Addr, r.probe.Colo, r.med, r.jit, r.speed.Mbps), now)
			d.store.Log(map[string]any{"type": "role", "ip": ip.Addr, "to": "active",
				"med_ms": r.med, "jitter_ms": r.jit, "mbps": r.speed.Mbps, "colo": r.probe.Colo, "t": now})
		} else if r.ok {
			d.logEvent(fmt.Sprintf("%s promotion passed but not applicable now (actives full or diversity)", r.ip), now)
		} else {
			// Bound the retry: back this standby off instead of re-running a
			// 99MB test every tick; other valid backups are tried meanwhile.
			d.promoteFail[ip.Addr] = now
			d.logEvent(fmt.Sprintf("%s promotion failed: %s", r.ip, r.err), now)
		}
	case jPK:
		d.pkRunning = false
		d.applyPK(r, now)
	case jSpeed:
		d.applySpeed(r, now)
	case jRelay:
		if ip == nil || ip.Role != RoleActive {
			return
		}
		st := d.relayStat(r.ip)
		st.Total++
		if r.probe.OK {
			// TCP dial success feeds the connect EMA and traffic stats only.
			// It is not HTTP/TLS/colo evidence and never restores health.
			if st.EMAms == 0 {
				st.EMAms = r.probe.Totalms
			} else {
				st.EMAms = 0.7*st.EMAms + 0.3*r.probe.Totalms
			}
		} else {
			st.Fails++
			// Real-traffic fast failure path: keep the TCP burst evidence
			// independent of HTTP counters, expedite verification, and let a
			// burst of dial failures degrade even before the next probe.
			ip.RelayFail++
			ip.LastRelayFail = now
			ip.NextProbe = now
			if ip.State == StateHealthy && ip.RelayFail >= d.cfg.DegradedFails {
				ip.State = StateDegraded
				ip.DegradedSince = now
				ip.ConsecOK, ip.StableOK, ip.Spikes = 0, 0, 0
				d.logEvent(fmt.Sprintf("%s degraded: %d consecutive relay dial failures", ip.Addr, ip.RelayFail), now)
			}
		}
	}

	if r.job.kind != jPromote && r.job.kind != jPK && r.job.kind != jSpeed {
		d.store.Log(map[string]any{
			"type": "obs", "kind": r.job.kind.String(), "ip": r.ip,
			"ok": r.probe.OK, "rtt_ms": r.probe.Totalms, "colo": r.probe.Colo,
			"err": r.probe.Err, "t": now,
		})
		if r.job.kind != jRelay {
			d.countProbe(r.job.kind, 1)
		}
	}
	d.updateRelaySnap()
}

// countPhysical truthfully accounts every physical request of long jobs:
// per-probe results feed prefix learning and (when current) the participant
// windows; bandwidth downloads are counted with their bytes.
func (d *daemon) countPhysical(r jobResult, now time.Time, ip *IP) {
	recordLeg := func(addr string, probes []ProbeResult, participant *IP) {
		for _, p := range probes {
			d.scan.Record(addr, p, now)
			d.countProbe(r.job.kind, 1)
			if participant != nil {
				d.pool.recordWin(participant, Obs{OK: p.OK, RTT: p.Totalms, Colo: p.Colo, T: p.T})
			}
		}
	}
	switch r.job.kind {
	case jPromote:
		recordLeg(r.ip, r.probes, ip)
		if r.speed.Bytes > 0 || r.speed.Err != "" {
			d.countProbe(jSpeed, 1)
			d.pool.dayBytes += r.speed.Bytes
		}
	case jPK:
		var inc *IP
		if r.peer != "" {
			inc = d.pool.get(r.peer)
		}
		recordLeg(r.ip, r.probes, ip)
		recordLeg(r.peer, r.peerProbes, inc)
	case jSpeed:
		d.countProbe(jSpeed, 2)
		d.pool.dayBytes += r.speed.Bytes + r.speed2.Bytes
	}
}

// countProbe adds to the persisted per-day kind counters.
func (d *daemon) countProbe(k jobKind, n int) {
	day := time.Now().Format("2006-01-02")
	if d.pool.day != day {
		d.pool.day, d.pool.dayCounts, d.pool.dayBytes = day, map[string]int{}, 0
		d.dayCounts = d.pool.dayCounts
	}
	d.pool.dayCounts[k.String()] += n
	d.hourCounts[k.String()] += n
}

// candidateRoom enforces the cap and the per-prefix diversity bound at
// admission time.
func (d *daemon) candidateRoom(addr string) bool {
	cands := d.pool.candidates()
	if len(cands) >= d.cfg.CandidateCap {
		return false
	}
	key, n := BlockKey(addr), 0
	for _, c := range cands {
		if BlockKey(c.Addr) == key {
			n++
		}
	}
	return n < d.cfg.CandidatePerPrefix
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
	j := job{kind: jSpeed, ip: r.ip, peer: r.peer, gen: d.pool.gen}
	log.Printf("PK %s confirmed (%s), bandwidth check (%d/2 today)", r.ip, why, d.pool.pk.BWUsed)
	go func() { d.sendResult(d.execSpeed(d.ctx, j)) }()
}

// applySpeed takes the final PK decision. Both serial measurements must be
// valid with acceptable colos — a missing incumbent reference is inconclusive,
// never a win. Roles, capacity and diversity are revalidated at commit time.
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
	if len(d.pool.actives()) != d.cfg.ActiveCount {
		return // pool shape changed since dispatch; an elective swap must not shrink it
	}
	others := make([]*IP, 0, d.cfg.ActiveCount-1)
	for _, a := range d.pool.actives() {
		if a != inc {
			others = append(others, a)
		}
	}
	win := r.speed.Err == "" && r.speed2.Err == "" &&
		r.speed.Mbps >= d.cfg.PromoteMinMbps && r.speed2.Mbps > 0 &&
		r.speed.Mbps >= 0.8*r.speed2.Mbps &&
		(r.speed.Colo == "" || d.cfg.coloAllowed(r.speed.Colo)) &&
		(r.speed2.Colo == "" || d.cfg.coloAllowed(r.speed2.Colo)) &&
		d.pool.diverseWith(sb, others)
	d.store.Log(map[string]any{"type": "pk_speed", "challenger": r.ip, "incumbent": r.peer,
		"chall_mbps": r.speed.Mbps, "inc_mbps": r.speed2.Mbps, "chall_err": r.speed.Err,
		"inc_err": r.speed2.Err, "win": win, "t": now})
	if !win {
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
	sb.Speed, sb.SpeedAt = r.speed.Mbps, now
	inc.Speed, inc.SpeedAt = r.speed2.Mbps, now
	sb.Colo = colo
	d.pool.swapPK(sb, inc, med, now)
	d.pkLastVerdict = fmt.Sprintf("%s -> %s swapped (%.1f vs %.1f Mbps)", r.peer, r.ip, r.speed.Mbps, r.speed2.Mbps)
	d.logEvent(fmt.Sprintf("PK swap: %s -> active, %s -> standby (%.1f vs %.1f Mbps)",
		sb.Addr, inc.Addr, r.speed.Mbps, r.speed2.Mbps), now)
}

func (d *daemon) logEvent(ev string, now time.Time) {
	log.Printf("%s", ev)
	d.store.Log(map[string]any{"type": "event", "msg": ev, "t": now})
}

// rollHour writes a compact hourly summary into the history stream.
func (d *daemon) rollHour(now time.Time) {
	if now.Truncate(time.Hour).Equal(d.hourStart) {
		return
	}
	entry := map[string]any{"type": "hourly", "from": d.hourStart, "counts": d.hourCounts, "t": now}
	meds := map[string]float64{}
	for _, a := range d.pool.actives() {
		meds[a.Addr] = a.medianRTT()
	}
	entry["active_medians_ms"] = meds
	d.store.Log(entry)
	d.hourStart = now.Truncate(time.Hour)
	d.hourCounts = map[string]int{}
}
