package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime/debug"
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
	jRecheck
)

func (k jobKind) String() string {
	return [...]string{"health", "standby", "validate", "discover", "promote", "pk", "speed", "relay", "recheck"}[k]
}

type job struct {
	kind    jobKind
	ip      string
	peer    string  // PK incumbent / speed comparison partner
	limit   float64 // median RTT ceiling snapshotted at dispatch
	gen     uint64  // dispatch-time generation of ip
	peerGen uint64  // dispatch-time generation of peer
	relax   bool    // fill promotion under a relaxed per-colo cap
	urgent  bool    // urgent PK: urgentProbes per side
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

// backoff delays the next promotion test of a standby that failed n times in
// a row.
type backoff struct {
	until time.Time
	n     int
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

	promoteFail map[string]backoff // bound promotion retries per standby

	rechecking bool
	lowBWTried map[string]time.Time // active -> SpeedAt of the sample whose replacement was tried

	relaySnap  atomic.Value // []relayTarget, written by event loop, read by relay goroutines
	relayStats map[string]*RelayStat
	clients    clientTable
	conns      connLog
	// halfCloseIdle ends a half-closed relay connection whose remaining
	// direction stays quiet this long.
	halfCloseIdle time.Duration
	lowMem        bool // the host is short of memory; see lowMemory
	ctx           context.Context
	discovering   int

	hourStart  time.Time
	hourCounts map[string]int
	dayCounts  map[string]int // mirrors pool.dayCounts (same map header)
}

func newDaemon(cfg *Config, store *Store) *daemon {
	d := &daemon{
		cfg:           cfg,
		store:         store,
		jobs:          make(chan job, 64),
		results:       make(chan jobResult, 64),
		inflight:      map[string]bool{},
		ctl:           make(chan ctlRequest),
		started:       time.Now(),
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		relayStats:    map[string]*RelayStat{},
		halfCloseIdle: time.Minute,
		pkFailUntil:   map[string]time.Time{},
		promoteFail:   map[string]backoff{},
		lowBWTried:    map[string]time.Time{},
		hourCounts:    map[string]int{},
		hourStart:     time.Now().Truncate(time.Hour),
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
	mem := time.NewTicker(time.Minute)
	defer mem.Stop()
	limitMB, low := tuneMemory(d.cfg)
	d.lowMem = low
	log.Printf("cfipd started, data=%s socket=%s memory_limit=%dMiB (0 = none)",
		d.cfg.DataDir, d.cfg.Socket, limitMB)
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
		case <-mem.C:
			was := d.lowMem
			if _, d.lowMem = tuneMemory(d.cfg); d.lowMem {
				d.shedMemory(time.Now(), !was)
			} else if was {
				log.Printf("memory is no longer low; candidate cap back to %d", d.cfg.CandidateCap)
			}
		case <-snap.C:
			d.pool.pruneCandidates(time.Now(), d.candidateCap())
			d.scan.prune(time.Now(), d.lowMem)
			if err := d.store.Save(d.pool, d.scan); err != nil {
				log.Printf("save: %v", err)
			}
		}
	}
}

// guard ends one unit of off-loop work (a job, a relay connection) that
// panicked. A panic there is a bug, but it must cost only that unit, not the
// daemon and its relay: guard logs it with the stack and calls failed, which
// turns the unit into a failed result. Fatal runtime errors, such as a
// concurrent map write, cannot be recovered; only keeping loop state on the
// loop prevents those.
func guard(what string, failed func()) {
	if v := recover(); v != nil {
		log.Printf("panic in %s: %v\n%s", what, v, debug.Stack())
		if failed != nil {
			failed()
		}
	}
}

// safeExec runs exec under guard: a panicking job reports as failed, so the
// event loop still clears its running flags.
func (d *daemon) safeExec(ctx context.Context, j job) (r jobResult) {
	defer guard(fmt.Sprintf("job %d for %s", j.kind, j.ip), func() { r = failedResult(j) })
	return d.exec(ctx, j)
}

// failedResult is the result of a job that panicked. Its downloads carry an
// error too: a zero SpeedResult would pass validSpeed as a 0 Mbps sample.
func failedResult(j job) jobResult {
	failed := SpeedResult{Err: "panic"}
	return jobResult{job: j, err: "panic", speed: failed, speed2: failed}
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
		case d.results <- d.safeExec(ctx, j):
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
	case jRecheck:
		return d.execRecheck(ctx, j)
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
	if d.pool.activeDegraded() { // refresh the standbys now, not on their normal schedule
		for _, sb := range sbs {
			if limit := now.Add(urgentStandbyEvery); sb.NextProbe.After(limit) {
				sb.NextProbe = limit
			}
		}
	}
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

	// Hard-failure fill has priority; PK may still improve a pool that fill
	// cannot complete.
	filling := false
	if d.longJobBlocked(now, gapFill) == "" && len(d.pool.actives()) < cfg.ActiveCount {
		filling = d.fillActive(now)
	}
	if !filling {
		d.replaceLowBandwidth(now)
		d.evalPK(now)
	}
	d.recheckBandwidth(now)
	d.rollHour(now)
	d.updateRelaySnap()
}

// longJobBlocked reports why no promotion, PK, recheck or replacement may
// start now, or "" if one may. One long job runs at a time, so bandwidth
// downloads never overlap; tests also start SpeedGap apart and pause after
// HTTP 429 to stay under Cloudflare rate limits. A fill with fewer than half
// of active_count left does not wait for the gap, since the relay is nearly
// empty; the 429 pause holds for every job.
// gapRule is how much of speed_gap a long job may skip.
type gapRule int

const (
	gapFull   gapRule = iota // every elective job
	gapFill                  // fill and manual promote: no gap while fewer than half the actives remain
	gapUrgent                // an urgent PK: urgentGap since the last download is enough
)

func (d *daemon) longJobBlocked(now time.Time, rule gapRule) string {
	lastDownload := d.pool.speedNext.Add(-d.cfg.SpeedGap.Duration)
	switch {
	case d.promoting:
		return "a promotion test is running"
	case d.pkRunning:
		return "a PK comparison is running"
	case d.rechecking:
		return "a bandwidth recheck is running"
	case now.Before(d.pool.speedPaused):
		return "bandwidth downloads paused after HTTP 429 until " + d.pool.speedPaused.Format(time.RFC3339)
	case now.Before(d.pool.speedNext) && !(rule == gapFill && 2*len(d.pool.actives()) < d.cfg.ActiveCount) &&
		!(rule == gapUrgent && !now.Before(lastDownload.Add(urgentGap))):
		return "next bandwidth download allowed at " + d.pool.speedNext.Format(time.RFC3339)
	}
	return ""
}

// noteDownload spaces the next bandwidth test SpeedGap after this download,
// and pauses all tests for at least an hour when Cloudflare answered HTTP 429.
func (d *daemon) noteDownload(now time.Time, rs ...SpeedResult) {
	if next := now.Add(d.cfg.SpeedGap.Duration); next.After(d.pool.speedNext) {
		d.pool.speedNext = next
	}
	for _, r := range rs {
		if r.Status == http.StatusTooManyRequests {
			d.pool.speedPaused = now.Add(max(time.Hour, d.cfg.SpeedGap.Duration))
			d.logEvent(fmt.Sprintf("bandwidth download rate-limited (HTTP 429); tests paused until %s",
				d.pool.speedPaused.Format(time.RFC3339)), now)
			return
		}
	}
}

// backOffPromotion doubles a failing standby's retry delay from 10 minutes up
// to a day, so one bad backup cannot spend a download every few minutes.
func (d *daemon) backOffPromotion(addr string, now time.Time) {
	b := d.promoteFail[addr]
	b.n++
	b.until = now.Add(min(10*time.Minute<<min(b.n-1, 8), 24*time.Hour)) // 10m<<8 > 24h; no overflow
	d.promoteFail[addr] = b
}

func sortByNextProbe(ips []*IP) {
	for i := 1; i < len(ips); i++ {
		for j := i; j > 0 && ips[j].NextProbe.Before(ips[j-1].NextProbe); j-- {
			ips[j], ips[j-1] = ips[j-1], ips[j]
		}
	}
}

// fillActive tries standbys best-first; a failed promotion backs off (see
// backOffPromotion) instead of re-running a 99MB test every tick. The per-colo cap relaxes only
// when no backup within it can be tried, so a single reachable colo still
// fills the pool. It reports whether fill work is in progress.
func (d *daemon) fillActive(now time.Time) bool {
	for _, relax := range []bool{false, true} {
		fits := func(sb *IP) bool { return d.pool.diverseWith(sb, d.pool.actives(), relax) }
		if d.tryPromote(now, fits, job{relax: relax}) {
			return true
		}
	}
	return false
}

// tryPromote starts a promotion test for the best standby that fits, skipping
// backups that are busy or backed off. A stale backup is refreshed first. It
// reports whether it found a standby to work on.
func (d *daemon) tryPromote(now time.Time, fits func(*IP) bool, j job) bool {
	for _, sb := range d.pool.rankedStandbys() {
		if sb.Promoting {
			continue
		}
		if now.Before(d.promoteFail[sb.Addr].until) {
			continue
		}
		if !fits(sb) {
			continue
		}
		if now.Sub(sb.LastSeen) > 2*time.Minute {
			if d.enqueue(job{kind: jStandby, ip: sb.Addr, gen: sb.Gen}) {
				sb.NextProbe = now.Add(d.pool.jitterDown(d.pool.standbyEvery(sb)))
			}
			return true // refresh the stale backup before trusting it
		}
		d.startPromotion(sb, j)
		return true
	}
	return false
}

// startPromotion runs the promotion test on a dedicated goroutine: it must
// never wait in the shared job queue behind discovery floods.
func (d *daemon) startPromotion(sb *IP, j job) {
	sb.Promoting = true
	d.promoting = true
	j.kind, j.ip, j.limit, j.gen = jPromote, sb.Addr, d.pool.rttLimit(), sb.Gen
	log.Printf("promotion test: %s (colo=%s med=%.0fms)", sb.Addr, sb.Colo, sb.medianRTT())
	go func() { d.sendResult(d.safeExec(d.ctx, j)) }()
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

func (d *daemon) apply(r jobResult) {
	if r.job.kind != jRelay {
		delete(d.inflight, r.job.kind.String()+r.ip)
	}
	if r.job.kind == jDiscover {
		d.discovering--
	}
	now := time.Now()
	ip := d.pool.get(r.ip)

	// Stale evidence is dropped: a result is scoped to the generation of each
	// IP it is about, so unrelated pool churn does not void it. Pool shape is
	// revalidated at commit time. Generation zero is a real captured
	// generation, not an opt-out.
	stale := false
	if ip != nil && r.job.kind != jDiscover {
		stale = r.job.gen != ip.Gen
	}
	if r.peer != "" {
		if peer := d.pool.get(r.peer); peer == nil || peer.Gen != r.job.peerGen {
			stale = true
		}
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
		if r.job.kind == jPromote {
			d.promoting = false
			if ip != nil {
				ip.Promoting = false
			}
		}
		if r.job.kind == jPK || r.job.kind == jSpeed {
			d.pkRunning = false
		}
		if r.job.kind == jRecheck {
			d.rechecking = false
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
		inc := d.pool.get(r.peer) // set when the test replaces a below-floor active
		switch {
		case !r.ok:
			// Bound the retry: back this standby off instead of re-running a
			// 99MB test every tick; other valid backups are tried meanwhile.
			// A 429 is the endpoint's limit, not the standby's; the pause in
			// noteDownload covers it.
			if r.speed.Status != http.StatusTooManyRequests {
				d.backOffPromotion(ip.Addr, now)
			}
			d.logEvent(fmt.Sprintf("%s promotion failed: %s", r.ip, r.err), now)
		case ip.Role == RoleStandby && inc != nil && inc.Role == RoleActive && inc.belowFloor() &&
			d.pool.swapDiverse(ip, inc):
			delete(d.promoteFail, ip.Addr)
			d.pool.recordSpeed(ip, r.speed.Mbps, now)
			d.pool.swapPK(ip, inc, r.med, now)
			d.logEvent(fmt.Sprintf("%s replaced below-floor active %s (%.1f vs %.1f Mbps)",
				ip.Addr, inc.Addr, r.speed.Mbps, inc.Speed), now)
			d.store.Log(map[string]any{"type": "role", "ip": ip.Addr, "to": "active", "replaces": inc.Addr,
				"med_ms": r.med, "jitter_ms": r.jit, "mbps": r.speed.Mbps, "colo": r.probe.Colo, "t": now})
		case ip.Role == RoleStandby && inc == nil && len(d.pool.actives()) < d.cfg.ActiveCount &&
			d.pool.diverseWith(ip, d.pool.actives(), r.relax):
			delete(d.promoteFail, ip.Addr)
			d.pool.recordSpeed(ip, r.speed.Mbps, now)
			d.pool.promoteActive(ip, r.med, now)
			d.logEvent(fmt.Sprintf("%s promoted -> active (colo=%s med=%.0fms jit=%.0fms speed=%.1fMbps)",
				ip.Addr, r.probe.Colo, r.med, r.jit, r.speed.Mbps), now)
			d.store.Log(map[string]any{"type": "role", "ip": ip.Addr, "to": "active",
				"med_ms": r.med, "jitter_ms": r.jit, "mbps": r.speed.Mbps, "colo": r.probe.Colo, "t": now})
		default:
			d.logEvent(fmt.Sprintf("%s promotion passed but not applicable now (pool changed)", r.ip), now)
		}
	case jPK:
		d.pkRunning = false
		d.applyPK(r, now)
	case jSpeed:
		d.applySpeed(r, now)
	case jRecheck:
		d.applyRecheck(r, ip, now)
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

	if r.job.kind != jPromote && r.job.kind != jPK && r.job.kind != jSpeed && r.job.kind != jRecheck {
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
			d.noteDownload(now, r.speed)
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
		d.noteDownload(now, r.speed, r.speed2)
	case jRecheck:
		if r.speed.Bytes > 0 || r.speed.Err != "" {
			d.countProbe(jSpeed, 1)
			d.pool.dayBytes += r.speed.Bytes
			d.noteDownload(now, r.speed)
		}
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
	if len(cands) >= d.candidateCap() {
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
