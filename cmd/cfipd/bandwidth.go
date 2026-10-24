package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// recheckBandwidth re-measures one active whose SpeedDue has passed (never
// measured: due at once). It waits for the long-job slot and the download
// gap, so it never overlaps another bandwidth download.
func (d *daemon) recheckBandwidth(now time.Time) {
	if d.cfg.SpeedRecheck.Duration <= 0 || d.longJobBlocked(now, false) != "" {
		return
	}
	for _, a := range d.pool.actives() {
		if now.Before(a.SpeedDue) {
			continue
		}
		a.SpeedDue = d.pool.recheckAfter(a, now) // a failed download retries on the same schedule
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

// validSpeed reports whether a download result is a usable bandwidth sample.
func (d *daemon) validSpeed(s SpeedResult) bool {
	return s.Err == "" && (s.Colo == "" || d.cfg.coloAllowed(s.Colo))
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
	if ip == nil || ip.Role != RoleActive || !d.validSpeed(r.speed) {
		return
	}
	d.pool.recordSpeed(ip, r.speed.Mbps, now)
	switch {
	case ip.BelowFloorN == 1:
		d.logEvent(fmt.Sprintf("%s bandwidth %.1fMbps < %.1fMbps floor; confirming in about an hour",
			ip.Addr, ip.Speed, d.cfg.PromoteMinMbps), now)
	case ip.belowFloor() && d.pool.floorReachable():
		d.logEvent(fmt.Sprintf("%s bandwidth %.1fMbps < %.1fMbps floor (confirmed); seeking a replacement",
			ip.Addr, ip.Speed, d.cfg.PromoteMinMbps), now)
	case ip.belowFloor():
		d.logEvent(fmt.Sprintf("%s bandwidth %.1fMbps < %.1fMbps floor (confirmed); no active reaches the floor, so nothing changes",
			ip.Addr, ip.Speed, d.cfg.PromoteMinMbps), now)
	}
}

// replaceLowBandwidth tries to replace the slowest confirmed below-floor
// active while another active shows the floor is reachable. The node keeps
// serving, with a quarter of the relay weight, until a standby passes the
// full promotion test; the swap is one-for-one. Each below-floor sample
// triggers at most one attempt, so downloads stay bounded by SpeedRecheck.
func (d *daemon) replaceLowBandwidth(now time.Time) {
	if d.longJobBlocked(now, false) != "" || !d.pool.floorReachable() {
		return
	}
	var low *IP
	for _, a := range d.pool.actives() {
		if a.belowFloor() && !d.lowBWTried[a.Addr].Equal(a.SpeedAt) && (low == nil || a.Speed < low.Speed) {
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
