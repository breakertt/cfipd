package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// recheckBandwidth re-measures one active whose latest bandwidth sample is
// older than SpeedRecheck. Only one bandwidth download runs at a time, so a
// recheck never overlaps a promotion or PK measurement.
func (d *daemon) recheckBandwidth(now time.Time) {
	every := d.cfg.SpeedRecheck.Duration
	if every <= 0 || d.promoting || d.pkRunning || d.rechecking {
		return
	}
	for _, a := range d.pool.actives() {
		last := a.SpeedAt
		if t := d.recheckAt[a.Addr]; t.After(last) {
			last = t // a failed download waits a full interval too
		}
		if now.Sub(last) < every {
			continue
		}
		d.recheckAt[a.Addr] = now
		d.rechecking = true
		j := job{kind: jRecheck, ip: a.Addr, gen: a.Gen}
		log.Printf("bandwidth recheck: %s (last %.1fMbps)", a.Addr, a.Speed)
		go func() { d.sendResult(d.execRecheck(d.ctx, j)) }()
		return
	}
}

func (d *daemon) execRecheck(ctx context.Context, j job) jobResult {
	res := jobResult{job: j, speed: speedTest(ctx, d.cfg, j.ip)}
	if ctx.Err() != nil {
		res.err = "cancelled"
	}
	res.ok = res.speed.Err == ""
	return res
}

// applyRecheck records a fresh bandwidth sample of an active. A failed or
// wrong-colo download keeps the previous sample.
func (d *daemon) applyRecheck(r jobResult, ip *IP, now time.Time) {
	d.rechecking = false
	if r.err == "cancelled" {
		return
	}
	d.store.Log(map[string]any{"type": "speed_check", "ip": r.ip, "mbps": r.speed.Mbps,
		"bytes": r.speed.Bytes, "colo": r.speed.Colo, "err": r.speed.Err, "t": now})
	if ip == nil || ip.Role != RoleActive || r.speed.Err != "" ||
		(r.speed.Colo != "" && !d.cfg.coloAllowed(r.speed.Colo)) {
		return
	}
	ip.Speed, ip.SpeedAt = r.speed.Mbps, now
	if ip.belowFloor(d.cfg) {
		d.logEvent(fmt.Sprintf("%s bandwidth %.1fMbps < %.1fMbps floor; seeking a replacement",
			ip.Addr, ip.Speed, d.cfg.PromoteMinMbps), now)
	}
}

// replaceLowBandwidth tries to replace the slowest below-floor active. The
// node keeps serving, with a quarter of the relay weight, until a standby
// passes the full promotion test; the swap is one-for-one. Each below-floor
// sample triggers at most one attempt, so downloads stay bounded by
// SpeedRecheck.
func (d *daemon) replaceLowBandwidth(now time.Time) {
	if d.promoting || d.pkRunning || d.rechecking {
		return
	}
	var low *IP
	for _, a := range d.pool.actives() {
		if a.belowFloor(d.cfg) && !d.lowBWTried[a.Addr].Equal(a.SpeedAt) && (low == nil || a.Speed < low.Speed) {
			low = a
		}
	}
	if low == nil {
		return
	}
	fits := func(sb *IP) bool { return d.pool.swapDiverse(sb, low) }
	if d.tryPromote(now, fits, job{peer: low.Addr, peerGen: low.Gen}) && d.promoting {
		d.lowBWTried[low.Addr] = low.SpeedAt
	}
}
