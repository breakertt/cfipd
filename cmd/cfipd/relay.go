package main

import (
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
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

// maxClients bounds the client table: the relay port can face the internet.
const maxClients = 256

// clientTable counts relay use per client IP since start. Relay goroutines
// write it under mu; bytes are atomic because the copy loops add to them.
type clientTable struct {
	mu sync.Mutex
	m  map[string]*clientStat
}

type clientStat struct {
	total, open, fails int64     // guarded by clientTable.mu
	last               time.Time // latest open or close; guarded by clientTable.mu
	up, down           atomic.Int64
}

type clientView struct {
	Addr      string `json:"addr"`
	Total     int64  `json:"total"`
	Open      int64  `json:"open"`
	Fails     int64  `json:"fails"`
	BytesUp   int64  `json:"bytes_up"`
	BytesDown int64  `json:"bytes_down"`
	Last      string `json:"last_seen"`
}

// open records a new connection from addr. A full table drops the idle client
// seen longest ago; if every client is busy, the connection is counted in a
// stat outside the table.
func (t *clientTable) open(addr string, now time.Time) *clientStat {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.m[addr]
	if c == nil {
		c = &clientStat{}
		if t.m == nil {
			t.m = map[string]*clientStat{}
		}
		if len(t.m) < maxClients || t.evictIdle() {
			t.m[addr] = c
		}
	}
	c.total++
	c.open++
	c.last = now
	return c
}

func (t *clientTable) evictIdle() bool {
	var old *clientStat
	var oldAddr string
	for a, c := range t.m {
		if c.open == 0 && (old == nil || c.last.Before(old.last)) {
			old, oldAddr = c, a
		}
	}
	if old != nil {
		delete(t.m, oldAddr)
	}
	return old != nil
}

func (t *clientTable) done(c *clientStat, failed bool, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c.open--
	c.last = now
	if failed {
		c.fails++
	}
}

// list returns the clients, last seen first.
func (t *clientTable) list() []clientView {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]clientView, 0, len(t.m))
	for a, c := range t.m {
		out = append(out, clientView{a, c.total, c.open, c.fails, c.up.Load(), c.down.Load(), c.last.Format(time.RFC3339)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Last > out[j].Last })
	return out
}

// recentConns bounds the closed connections that connLog keeps.
const recentConns = 100

// connLog lists each open relay connection and the most recent closed ones.
// Relay goroutines write it under mu; bytes are atomic because the copy loops
// add to them. An open connection already holds two copy buffers, so its
// record adds little.
type connLog struct {
	mu     sync.Mutex
	open   map[*connRecord]struct{}
	recent []*connRecord // ring of closed connections, oldest first
}

type connRecord struct {
	client             string    // ip:port of the client
	upstream, err      string    // guarded by connLog.mu
	start, end         time.Time // end guarded by connLog.mu; zero while open
	bytesUp, bytesDown atomic.Int64
}

type connView struct {
	Client    string  `json:"client"`
	Upstream  string  `json:"upstream,omitempty"`
	Start     string  `json:"start"`
	End       string  `json:"end,omitempty"`
	Seconds   float64 `json:"seconds"`
	BytesUp   int64   `json:"bytes_up"`
	BytesDown int64   `json:"bytes_down"`
	Err       string  `json:"error,omitempty"`
}

func (l *connLog) start(client string, now time.Time) *connRecord {
	r := &connRecord{client: client, start: now}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.open == nil {
		l.open = map[*connRecord]struct{}{}
	}
	l.open[r] = struct{}{}
	return r
}

func (l *connLog) dialed(r *connRecord, upstream string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.upstream = upstream
}

func (l *connLog) end(r *connRecord, err string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.open, r)
	r.end, r.err = now, err
	if len(l.recent) == recentConns {
		l.recent = append(l.recent[:0], l.recent[1:]...)
	}
	l.recent = append(l.recent, r)
}

// list returns the open connections, newest first, then the closed ones,
// newest first.
func (l *connLog) list() []connView {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	view := func(r *connRecord) connView {
		v := connView{Client: r.client, Upstream: r.upstream, Start: r.start.Format(time.RFC3339),
			BytesUp: r.bytesUp.Load(), BytesDown: r.bytesDown.Load(), Err: r.err}
		end := now
		if !r.end.IsZero() {
			end, v.End = r.end, r.end.Format(time.RFC3339)
		}
		v.Seconds = end.Sub(r.start).Seconds()
		return v
	}
	out := make([]connView, 0, len(l.open)+len(l.recent))
	for r := range l.open {
		out = append(out, view(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seconds < out[j].Seconds })
	for i := len(l.recent) - 1; i >= 0; i-- {
		out = append(out, view(l.recent[i]))
	}
	return out
}

// relayBufs holds the copy buffers of relay connections.
var relayBufs = sync.Pool{New: func() any { b := make([]byte, 16<<10); return &b }}

// copyCounted copies src to dst through a pooled 16 KiB buffer (one full TLS
// record per read) and counts the
// bytes into each counter. Wrapping src hides net.TCPConn's WriteTo, which would allocate its
// own 32 KiB buffer for every direction of every connection.
func copyCounted(dst io.Writer, src io.Reader, ns ...*atomic.Int64) {
	buf := relayBufs.Get().(*[]byte)
	defer relayBufs.Put(buf)
	io.CopyBuffer(countWriter{dst, ns}, struct{ io.Reader }{src}, *buf)
}

type countWriter struct {
	w  io.Writer
	ns []*atomic.Int64
}

func (cw countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	for _, c := range cw.ns {
		c.Add(int64(n))
	}
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
	cut, limit := d.pool.floorReachable(), d.pool.rttLimit()
	actives := d.pool.actives()
	var speeds []float64
	for _, a := range actives {
		if a.Speed > 0 {
			speeds = append(speeds, a.Speed)
		}
	}
	ref := 1.0 // an active without a speed sample scores as the median active
	if len(speeds) > 0 {
		ref = median(speeds)
	}
	for _, a := range actives {
		st := d.relayStat(a.Addr)
		lat := a.medianRTT() // probe metric for every active; relay EMA is TCP dial time
		if lat <= 0 {
			lat = 500
		}
		bw := a.Speed
		if bw <= 0 {
			bw = ref
		}
		if cut { // under the floor the floor rule acts, after confirmation
			bw = max(bw, d.cfg.PromoteMinMbps)
		}
		w := score(d.cfg.BandwidthWeight, lat, bw)
		if a.State == StateDegraded || (cut && a.belowFloor()) || lat > limit {
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
	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		host = c.RemoteAddr().String()
	}
	cs, served := d.clients.open(host, time.Now()), false
	cr, why := d.conns.start(c.RemoteAddr().String(), time.Now()), "no active"
	defer func() {
		if served {
			why = ""
		}
		now := time.Now()
		d.clients.done(cs, !served, now)
		d.conns.end(cr, why, now)
	}()
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
			why = err.Error()
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
	served = true
	d.conns.dialed(cr, chosen.Addr)
	defer up.Close()
	defer chosen.Stat.Conns.Add(-1)

	st := chosen.Stat
	done := make(chan struct{}, 2)
	go func() {
		copyCounted(up, c, &st.BytesUp, &cs.up, &cr.bytesUp)
		if tc, ok := up.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		copyCounted(c, up, &st.BytesDown, &cs.down, &cr.bytesDown)
		if tc, ok := c.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}()
	<-done
}
