package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

type Role string

const (
	RoleCandidate Role = "candidate"
	RoleStandby   Role = "standby"
	RoleActive    Role = "active"
	// RoleQuarantine is load-compat only: v2 migrates it to a Candidate cooldown.
	RoleQuarantine Role = "quarantine"
)

const (
	StateHealthy  = "healthy"
	StateDegraded = "degraded"
)

type Obs struct {
	OK   bool      `json:"ok"`
	RTT  float64   `json:"rtt"`
	Colo string    `json:"colo"`
	T    time.Time `json:"t"`
}

type IP struct {
	Addr string `json:"addr"`
	Role Role   `json:"role"`

	State    string  `json:"state,omitempty"`
	Colo     string  `json:"colo,omitempty"`
	Baseline float64 `json:"baseline_ms,omitempty"`

	// Win holds the most recent real observations, successes and failures,
	// each with its true sample time. It is the single source of truth for
	// median/jitter/failure-rate across all roles.
	Win []Obs `json:"win,omitempty"`

	ConsecFail int `json:"consec_fail"`
	ConsecOK   int `json:"consec_ok"`
	HardFails  int `json:"hard_fails"`
	ColoBad    int `json:"colo_bad"`
	Spikes     int `json:"spikes"`
	StableOK   int `json:"stable_ok"` // consecutive genuinely stable active probes

	DegradedSince time.Time `json:"degraded_since,omitempty"`

	// RelayFail/LastRelayFail retain independent real-traffic TCP dial burst
	// evidence; HTTP success never erases a newer dial failure.
	RelayFail     int       `json:"relay_fail,omitempty"`
	LastRelayFail time.Time `json:"last_relay_fail,omitempty"`

	Speed   float64   `json:"speed_mbps,omitempty"`
	SpeedAt time.Time `json:"speed_at,omitempty"`
	// BelowFloorN counts consecutive bandwidth samples under the promotion
	// floor; SpeedDue is when an active's next bandwidth recheck is due.
	BelowFloorN int       `json:"below_floor_n,omitempty"`
	SpeedDue    time.Time `json:"speed_due,omitempty"`

	NextProbe     time.Time `json:"next_probe"`
	CooldownUntil time.Time `json:"cooldown_until,omitempty"`
	// CycleSince starts the current validation round; FirstSeen stays the
	// first-ever discovery time and is never overwritten.
	CycleSince time.Time `json:"cycle_since,omitempty"`

	// Gen is per-IP scoped evidence for every async job about this IP: a late
	// result from before its last role change must not poison it.
	Gen       uint64 `json:"gen,omitempty"`
	Promoting bool   `json:"promoting,omitempty"`

	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`

	// Legacy v1 fields, migrated on load and never written by v2 logic.
	QuarUntil time.Time `json:"quar_until,omitempty"`
}

func (ip *IP) okRTTs() []float64 {
	var xs []float64
	for _, o := range ip.Win {
		if o.OK {
			xs = append(xs, o.RTT)
		}
	}
	return xs
}

func (ip *IP) medianRTT() float64 { return median(ip.okRTTs()) }

// belowFloor reports whether two consecutive bandwidth samples fell under the
// promotion floor: one short sample is not enough to act on.
func (ip *IP) belowFloor() bool { return ip.BelowFloorN >= 2 }

func (ip *IP) jitter() float64 {
	xs := ip.okRTTs()
	if len(xs) < 2 {
		return 0
	}
	return pct90(xs) - median(xs)
}

// successRate reports the probe failure evidence; ok=false when unsampled so
// callers can render "unknown" instead of a fake 100% loss.
func (ip *IP) successRate() (float64, bool) {
	if len(ip.Win) == 0 {
		return 0, false
	}
	ok := 0
	for _, o := range ip.Win {
		if o.OK {
			ok++
		}
	}
	return float64(ok) / float64(len(ip.Win)), true
}

// windowStart is the time of the oldest retained observation.
func (ip *IP) windowStart() time.Time {
	if len(ip.Win) == 0 {
		return time.Time{}
	}
	return ip.Win[0].T
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func pct90(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[int(0.9*float64(len(s)-1))]
}

type Pool struct {
	cfg *Config
	ips map[string]*IP
	rnd *rand.Rand
	pk  *pkPersist

	speedNext time.Time // earliest next bandwidth download (SpeedGap, HTTP 429 pause)

	day       string         // UTC day of dayCounts/dayBytes
	dayCounts map[string]int // physical probes per kind today
	dayBytes  int64          // bandwidth test bytes today
}

func NewPool(cfg *Config) *Pool {
	return &Pool{cfg: cfg, ips: map[string]*IP{}, dayCounts: map[string]int{},
		rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (p *Pool) get(addr string) *IP { return p.ips[addr] }

func (p *Pool) ensure(addr string, now time.Time) *IP {
	if ip, ok := p.ips[addr]; ok {
		return ip
	}
	ip := &IP{Addr: addr, Role: RoleCandidate, FirstSeen: now, CycleSince: now, LastSeen: now}
	p.ips[addr] = ip
	return ip
}

func (p *Pool) remove(addr string) { delete(p.ips, addr) }

func (p *Pool) byRole(r Role) []*IP {
	var out []*IP
	for _, ip := range p.ips {
		if ip.Role == r {
			out = append(out, ip)
		}
	}
	return out
}

func (p *Pool) actives() []*IP    { return p.byRole(RoleActive) }
func (p *Pool) standbys() []*IP   { return p.byRole(RoleStandby) }
func (p *Pool) candidates() []*IP { return p.byRole(RoleCandidate) }

// rankedStandbys returns standbys best-first: success rate, then median RTT, then speed.
func (p *Pool) rankedStandbys() []*IP {
	sbs := p.standbys()
	sort.Slice(sbs, func(i, j int) bool {
		a, b := sbs[i], sbs[j]
		ra, oka := a.successRate()
		rb, okb := b.successRate()
		if !oka || !okb { // unsampled ranks last, not "100% loss"
			return oka && !okb
		}
		if ra != rb {
			return ra > rb
		}
		if a.medianRTT() != b.medianRTT() {
			return a.medianRTT() < b.medianRTT()
		}
		return a.Speed > b.Speed
	})
	return sbs
}

func (p *Pool) recordWin(ip *IP, o Obs) {
	ip.Win = append(ip.Win, o)
	if len(ip.Win) > p.cfg.WindowSize {
		ip.Win = ip.Win[len(ip.Win)-p.cfg.WindowSize:]
	}
}

// healthEvery is the adaptive per-active interval: anomalies recheck fast,
// sustained confirmation backs off up to 12x base. For a degraded node the
// streak is consecutive availability (it stays slow but confirmed), for a
// healthy node genuinely stable samples.
func (ip *IP) healthEvery(cfg *Config) time.Duration {
	base := cfg.HealthInterval.Duration
	if ip.ConsecFail > 0 || ip.Spikes > 0 {
		return 2 * time.Second
	}
	n := ip.StableOK
	if ip.State == StateDegraded {
		n = ip.ConsecOK
	}
	switch {
	case n >= 15:
		return 12 * base
	case n >= 10:
		return 6 * base
	case n >= 5:
		return 3 * base
	default:
		return base
	}
}

// jitterDown applies -20%..0 jitter: the nominal interval is a hard maximum.
func (p *Pool) jitterDown(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.2*p.rnd.Float64()))
}

// applyHealth feeds one health probe result into an active IP's state machine.
// Returns events worth logging.
func (p *Pool) applyHealth(ip *IP, r ProbeResult, now time.Time) []string {
	var ev []string
	cfg := p.cfg
	ip.LastSeen = now
	p.recordWin(ip, Obs{OK: r.OK, RTT: r.Totalms, Colo: r.Colo, T: now})

	degrade := func(reason string) {
		ip.State = StateDegraded
		ip.DegradedSince = now
		ip.ConsecOK, ip.StableOK, ip.Spikes = 0, 0, 0 // recovery is earned from scratch
		ev = append(ev, fmt.Sprintf("%s degraded: %s", ip.Addr, reason))
	}

	if r.OK {
		ip.ConsecFail = 0
		ip.ConsecOK++
		ip.HardFails = 0
		ip.Colo = r.Colo
		pt := r.T
		if pt.IsZero() {
			pt = now
		}
		if pt.After(ip.LastRelayFail) {
			ip.RelayFail = 0 // HTTP evidence newer than the dial failure supersedes it
		}
		if cfg.coloAllowed(r.Colo) {
			ip.ColoBad = 0
		} else {
			ip.ColoBad++
		}
		// stable means: right colo and RTT back near baseline. Recovery from
		// degraded requires it, so a still-slow node cannot flap healthy.
		stable := cfg.coloAllowed(r.Colo) && (ip.Baseline == 0 || r.Totalms <= 1.5*ip.Baseline)
		if stable {
			ip.StableOK++
		} else {
			ip.StableOK = 0
		}
		switch ip.State {
		case StateDegraded:
			// Recovery must be earned with consecutive genuinely stable
			// samples; successful-but-still-slow probes don't count.
			if ip.StableOK >= cfg.RecoverOKs && stable {
				ip.State = StateHealthy
				ip.DegradedSince = time.Time{}
				ip.Spikes = 0 // a future outlier must not reuse this episode's streak
				ev = append(ev, fmt.Sprintf("%s recovered -> healthy", ip.Addr))
			}
		case StateHealthy:
			if ip.Baseline == 0 || r.Totalms <= 2*ip.Baseline {
				ip.Baseline = ip.medianRTT()
			}
			if ip.Baseline > 0 && r.Totalms > 2*ip.Baseline {
				ip.Spikes++
				if ip.Spikes >= 2 {
					degrade(fmt.Sprintf("%d consecutive rtt spikes (%.0fms > 2x baseline %.0fms)", ip.Spikes, r.Totalms, ip.Baseline))
				}
			} else {
				ip.Spikes = 0
			}
		}
	} else {
		ip.ConsecOK = 0
		ip.ConsecFail++
		ip.StableOK = 0
		if r.Hard {
			ip.HardFails++
		} else {
			ip.HardFails = 0
		}
		if ip.State == StateHealthy && ip.ConsecFail >= cfg.DegradedFails {
			degrade(fmt.Sprintf("%d consecutive failures (%s)", ip.ConsecFail, r.Err))
		}
	}

	// recent-loss check: >=2 fails in last 6 obs while healthy
	if ip.State == StateHealthy && len(ip.Win) >= 6 {
		fails := 0
		for _, o := range ip.Win[len(ip.Win)-6:] {
			if !o.OK {
				fails++
			}
		}
		if fails >= 2 {
			degrade(fmt.Sprintf("%d failures in last 6 probes", fails))
		}
	}

	if down := p.checkDown(ip, now); down != "" {
		p.cooldown(ip, down, now)
		ev = append(ev, fmt.Sprintf("%s down -> candidate cooldown: %s", ip.Addr, down))
	}
	if ip.Role == RoleActive { // cooldown() already set NextProbe otherwise
		ip.NextProbe = now.Add(p.jitterDown(ip.healthEvery(cfg)))
	}
	return ev
}

// checkDown returns the reason an active should be pulled, or "".
// Slow-but-usable nodes are replaced atomically only after promotion/PK
// validation; backup observations alone cannot authorize withdrawing one.
func (p *Pool) checkDown(ip *IP, _ time.Time) string {
	switch {
	case ip.ConsecFail >= p.cfg.DownFails:
		return fmt.Sprintf("%d consecutive failures", ip.ConsecFail)
	case ip.HardFails >= p.cfg.HardFailDown:
		return "hard failure (TLS/refused)"
	case ip.ColoBad >= p.cfg.ColoBadDown:
		return fmt.Sprintf("colo left allowed set (now %s)", ip.Colo)
	}
	return ""
}

// applyStandby feeds one standby re-check result.
func (p *Pool) applyStandby(ip *IP, r ProbeResult, now time.Time) []string {
	ip.LastSeen = now
	p.recordWin(ip, Obs{OK: r.OK, RTT: r.Totalms, Colo: r.Colo, T: now})
	if r.Restricted { // hard failure: skip the two-strike rule
		p.cooldown(ip, r.Err, now)
		return []string{fmt.Sprintf("%s standby -> candidate cooldown (%s)", ip.Addr, r.Err)}
	}
	if r.OK && p.cfg.coloAllowed(r.Colo) {
		ip.ConsecFail = 0
		ip.Colo = r.Colo
	} else {
		ip.ConsecFail++
		if ip.ConsecFail >= 2 {
			reason := r.Err
			if r.OK {
				reason = "colo " + r.Colo + " not allowed"
			}
			p.cooldown(ip, reason, now)
			return []string{fmt.Sprintf("%s standby -> candidate cooldown (%s)", ip.Addr, reason)}
		}
	}
	if ip.Role == RoleStandby { // cooldown() already set NextProbe otherwise
		ip.NextProbe = now.Add(p.jitterDown(p.standbyEvery(ip)))
	}
	return nil
}

// standbyEvery: the best two standbys stay hot, the rest cold.
func (p *Pool) standbyEvery(ip *IP) time.Duration {
	ranked := p.rankedStandbys()
	for i, sb := range ranked {
		if sb == ip {
			if i < 2 {
				return p.cfg.StandbyCheck.Duration
			}
			break
		}
	}
	return p.cfg.StandbyCold.Duration
}

// bestActiveMedian returns the lowest median RTT among actives, or 0.
func (p *Pool) bestActiveMedian() float64 {
	best := 0.0
	for _, a := range p.actives() {
		if m := a.medianRTT(); m > 0 && (best == 0 || m < best) {
			best = m
		}
	}
	return best
}

// rttLimit floats with reality: 1.3x the best active. With no active reference
// there is no RTT veto at all — cold start must not reject every usable node.
func (p *Pool) rttLimit() float64 {
	if b := p.bestActiveMedian(); b > 0 {
		return 1.3 * b
	}
	return math.Inf(1)
}

// needCandidates reports whether candidate validation currently has a purpose:
// backup deficit, an unhealthy active, or a candidate that already looks like
// a credible improvement over the worst active.
func (p *Pool) needCandidates() bool {
	if len(p.actives()) < p.cfg.ActiveCount || len(p.standbys()) < p.cfg.StandbyTarget {
		return true
	}
	worst := 0.0
	for _, a := range p.actives() {
		if a.State != StateHealthy {
			return true
		}
		if m := a.medianRTT(); m > worst {
			worst = m
		}
	}
	if worst <= 0 {
		return false
	}
	for _, c := range p.candidates() {
		if m := c.medianRTT(); m > 0 && m <= worst-30 && m <= 0.8*worst {
			return true
		}
	}
	return false
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

// applyCandidate records one truthful validation observation. After
// ValidationObs samples the round concludes: graduate to standby if useful,
// otherwise leave the pool. No endless resets.
func (p *Pool) applyCandidate(ip *IP, r ProbeResult, now time.Time) []string {
	cfg := p.cfg
	ip.LastSeen = now
	p.recordWin(ip, Obs{OK: r.OK, RTT: r.Totalms, Colo: r.Colo, T: now})
	if r.OK {
		ip.Colo = r.Colo
	}
	if r.Restricted { // hard failure: no point in finishing the round
		p.remove(ip.Addr)
		return []string{fmt.Sprintf("%s candidate dropped: %s", ip.Addr, r.Err)}
	}

	if len(ip.Win) < cfg.ValidationObs {
		ip.NextProbe = now.Add(p.jitterDown(cfg.ValidationGap.Duration))
		return nil
	}

	fails, coloOK, oks := 0, 0, 0
	for _, o := range ip.Win {
		if o.OK {
			oks++
			if cfg.coloAllowed(o.Colo) {
				coloOK++
			}
		} else {
			fails++
		}
	}
	med := ip.medianRTT()
	pass := fails <= 1 && // one soft failure per round
		(oks == 0 || float64(coloOK)/float64(oks) >= 0.8) &&
		med > 0 && med < p.rttLimit() &&
		cfg.coloAllowed(ip.Colo)
	if !pass {
		p.remove(ip.Addr)
		return []string{fmt.Sprintf("%s candidate round failed, left pool", ip.Addr)}
	}

	sbs := p.standbys()
	if len(sbs) >= cfg.StandbyMax {
		worst := p.rankedStandbys()[len(sbs)-1]
		if med >= worst.medianRTT() {
			p.remove(ip.Addr)
			return []string{fmt.Sprintf("%s candidate passed but not needed, left pool (med=%.0fms)", ip.Addr, med)}
		}
		p.remove(worst.Addr)
	}
	ip.Role = RoleStandby
	ip.ConsecFail = 0
	ip.CooldownUntil = time.Time{}
	ip.NextProbe = now
	ip.Gen++
	return []string{fmt.Sprintf("%s candidate -> standby (colo=%s med=%.0fms)", ip.Addr, ip.Colo, med)}
}

// cooldown parks a failed node as a candidate with a bounded rest. It replaces
// the v1 quarantine role: no lifetime penalty, revalidation after Cooldown.
func (p *Pool) cooldown(ip *IP, reason string, now time.Time) {
	ip.Role = RoleCandidate
	ip.State = ""
	ip.CooldownUntil = now.Add(p.cfg.Cooldown.Duration)
	ip.NextProbe = ip.CooldownUntil
	ip.ConsecFail, ip.ConsecOK, ip.HardFails, ip.ColoBad, ip.Spikes, ip.StableOK = 0, 0, 0, 0, 0, 0
	ip.RelayFail = 0
	ip.DegradedSince = time.Time{}
	ip.Promoting = false
	ip.Win = nil
	ip.CycleSince = now // fresh validation window after cooldown; FirstSeen kept
	ip.Gen++
}

// promoteActive turns a standby into a healthy active after a passed test.
func (p *Pool) promoteActive(ip *IP, med float64, now time.Time) {
	ip.Role = RoleActive
	ip.State = StateHealthy
	ip.Promoting = false
	ip.ConsecFail, ip.ConsecOK, ip.Spikes, ip.StableOK = 0, 0, 0, 0
	ip.RelayFail = 0
	ip.Baseline = med
	ip.CooldownUntil = time.Time{}
	ip.DegradedSince = time.Time{}
	ip.NextProbe = now.Add(p.jitterDown(ip.healthEvery(p.cfg)))
	ip.Gen++
}

// demoteToStandby moves a healthy active back to standby after a PK swap:
// no punishment, observations kept, quick refresh.
func (p *Pool) demoteToStandby(ip *IP, now time.Time) {
	ip.Role = RoleStandby
	ip.State = ""
	ip.ConsecFail, ip.ConsecOK, ip.HardFails, ip.ColoBad, ip.Spikes, ip.StableOK = 0, 0, 0, 0, 0, 0
	ip.RelayFail = 0
	ip.DegradedSince = time.Time{}
	ip.NextProbe = now
	ip.Gen++
}

// recordSpeed stores a valid bandwidth sample, counts consecutive samples
// under the promotion floor and schedules the next recheck: about an hour
// after a first low sample to confirm it, else SpeedRecheck; both jittered so
// rechecks do not lock to fixed times of day.
func (p *Pool) recordSpeed(ip *IP, mbps float64, now time.Time) {
	ip.Speed, ip.SpeedAt = mbps, now
	if mbps < p.cfg.PromoteMinMbps {
		ip.BelowFloorN++
	} else {
		ip.BelowFloorN = 0
	}
	every := p.cfg.SpeedRecheck.Duration
	if ip.BelowFloorN == 1 {
		every = time.Hour
	}
	ip.SpeedDue = now.Add(p.jitterDown(every))
}

// swapPK atomically replaces incumbent active with challenger standby.
// Relay connections on the incumbent are untouched: they belong to accepted
// sockets, and only new dials consult the snapshot.
func (p *Pool) swapPK(challenger, incumbent *IP, med float64, now time.Time) {
	p.demoteToStandby(incumbent, now)
	p.promoteActive(challenger, med, now)
}

// diverseWith reports whether adding sb to the given actives respects the
// per-prefix cap and, unless relaxColo, the per-colo cap.
func (p *Pool) diverseWith(sb *IP, actives []*IP, relaxColo bool) bool {
	pfxN, coloN := 0, 0
	for _, a := range actives {
		if BlockKey(a.Addr) == BlockKey(sb.Addr) {
			pfxN++
		}
		if a.Colo == sb.Colo && sb.Colo != "" {
			coloN++
		}
	}
	return pfxN < p.cfg.MaxActivePerPrefix && (relaxColo || coloN < p.cfg.MaxActivePerColo)
}

// swapDiverse reports whether sb may take inc's active slot. A same-colo swap
// leaves colo counts unchanged, so only the per-prefix cap applies to it.
func (p *Pool) swapDiverse(sb, inc *IP) bool {
	var others []*IP
	for _, a := range p.actives() {
		if a != inc {
			others = append(others, a)
		}
	}
	return p.diverseWith(sb, others, sb.Colo != "" && sb.Colo == inc.Colo)
}

// pruneCandidates expires stale candidates and enforces the cap. Nodes in
// cooldown are kept so a failed active cannot be pruned and immediately
// rediscovered past its rest.
func (p *Pool) pruneCandidates(now time.Time) {
	for _, c := range p.candidates() {
		if c.CooldownUntil.After(now) {
			continue
		}
		since := c.CycleSince
		if since.IsZero() {
			since = c.FirstSeen
		}
		if now.Sub(since) > p.cfg.ValidationWindow.Duration {
			p.remove(c.Addr)
		}
	}
	cands := p.candidates()
	if len(cands) <= p.cfg.CandidateCap {
		return
	}
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		ra, oka := a.successRate()
		rb, okb := b.successRate()
		if oka != okb {
			return oka // unsampled pruned first
		}
		if ra != rb {
			return ra < rb
		}
		return a.LastSeen.Before(b.LastSeen)
	})
	for _, ip := range cands[:len(cands)-p.cfg.CandidateCap] {
		if ip.CooldownUntil.After(now) {
			continue
		}
		p.remove(ip.Addr)
	}
}
