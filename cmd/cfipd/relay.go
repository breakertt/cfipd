package main

import (
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

type RelayStat struct {
	Total     int64 // event-loop only
	Fails     int64 // event-loop only
	EMAms     float64
	BytesUp   atomic.Int64
	BytesDown atomic.Int64
	Conns     atomic.Int64
}

type relayTarget struct {
	Addr   string
	Weight float64
	Stat   *RelayStat
	Gen    uint64 // active generation at snapshot time; scopes dial feedback
}

type countWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (cw countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n.Add(int64(n))
	return n, err
}

func (d *daemon) relayStat(addr string) *RelayStat {
	st, ok := d.relayStats[addr]
	if !ok {
		st = &RelayStat{}
		d.relayStats[addr] = st
	}
	return st
}

// updateRelaySnap publishes a lock-free snapshot of current actives for relay
// handler goroutines. Event-loop only.
func (d *daemon) updateRelaySnap() {
	var ts []relayTarget
	for _, a := range d.pool.actives() {
		st := d.relayStat(a.Addr)
		lat := a.medianRTT() // probe metric for every active; relay EMA is TCP dial time
		if lat <= 0 {
			lat = 500
		}
		w := 1 / lat
		if a.State == StateDegraded || a.belowFloor() {
			w /= 4
		}
		ts = append(ts, relayTarget{Addr: a.Addr, Weight: w, Stat: st, Gen: a.Gen})
	}
	d.relaySnap.Store(ts)
}

func weightedOrder(ts []relayTarget) []relayTarget {
	order := append([]relayTarget(nil), ts...)
	for i := range order {
		total := 0.0
		for j := i; j < len(order); j++ {
			total += order[j].Weight
		}
		r, acc, pick := rand.Float64()*total, 0.0, i
		for j := i; j < len(order); j++ {
			acc += order[j].Weight
			if r <= acc {
				pick = j
				break
			}
		}
		order[i], order[pick] = order[pick], order[i]
	}
	return order
}

func (d *daemon) serveRelay(ctx context.Context) {
	if d.cfg.RelayListen == "" {
		return
	}
	ln, err := net.Listen("tcp", d.cfg.RelayListen)
	if err != nil {
		log.Printf("relay listen %s: %v", d.cfg.RelayListen, err)
		return
	}
	log.Printf("relay listening on %s, upstream port %d", d.cfg.RelayListen, d.cfg.RelayPort)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go d.handleRelayConn(c)
	}
}

func (d *daemon) handleRelayConn(c net.Conn) {
	defer c.Close()
	ts, _ := d.relaySnap.Load().([]relayTarget)
	if len(ts) == 0 {
		return
	}
	order := weightedOrder(ts)
	port := strconv.Itoa(d.cfg.RelayPort)

	var chosen *relayTarget
	var up net.Conn
	for i := range order {
		t := &order[i]
		t.Stat.Conns.Add(1)
		start := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(t.Addr, port), d.cfg.RelayDial.Duration)
		res := jobResult{job: job{kind: jRelay, ip: t.Addr, gen: t.Gen}}
		res.probe.OK = err == nil
		res.probe.Totalms = ms(time.Since(start))
		if err != nil {
			res.probe.Hard = classifyErr(err)
			res.probe.Err = err.Error()
			t.Stat.Conns.Add(-1)
		}
		select {
		case d.results <- res:
		default:
		}
		if err == nil {
			chosen, up = t, conn
			break
		}
	}
	if up == nil {
		return
	}
	defer up.Close()
	defer chosen.Stat.Conns.Add(-1)

	st := chosen.Stat
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(countWriter{up, &st.BytesUp}, c)
		if tc, ok := up.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		io.Copy(countWriter{c, &st.BytesDown}, up)
		if tc, ok := c.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	<-done
}
