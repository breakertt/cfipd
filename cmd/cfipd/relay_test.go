package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayFeedbackBoundaries(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "1.1.1.1", Role: RoleActive, State: StateDegraded, Colo: "NRT",
		ConsecOK: 0, NextProbe: t0.Add(time.Hour)}
	d.pool.ips[ip.Addr] = ip
	// TCP dial success: EMA only, never HTTP health recovery
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{OK: true, Totalms: 50}})
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{OK: true, Totalms: 60}})
	if ip.State != StateDegraded || ip.ConsecOK != 0 {
		t.Fatalf("relay TCP success must not restore HTTP health: state=%s consecOK=%d", ip.State, ip.ConsecOK)
	}
	// TCP dial failure: immediate faster verification
	far := time.Now().Add(time.Hour)
	ip.NextProbe = far
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: ProbeResult{Err: "refused"}})
	if !ip.NextProbe.Before(far) {
		t.Fatalf("relay failure should pull NextProbe to now, got %v", ip.NextProbe)
	}
	if d.relayStat(ip.Addr).Fails != 1 {
		t.Fatal("relay failure should count in relay stats")
	}
}

// TestRaceEventLoopVsReaders hammers the event-loop entry points and the
// relay snapshot reader concurrently; run with -race.
func TestRaceEventLoopVsReaders(t *testing.T) {
	d := testDaemon(t)
	d.cfg.ActiveCount = 0
	d.ctx = context.Background()
	for i := range 3 {
		d.pool.ips[fmt.Sprintf("1.1.1.%d", i+1)] = &IP{Addr: fmt.Sprintf("1.1.1.%d", i+1),
			Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				d.relaySnap.Load()
			}
		}
	}()
	for i := range 200 {
		now := t0.Add(time.Duration(i) * time.Second)
		d.schedule(now)
		d.apply(jobResult{job: job{kind: jRelay, ip: "1.1.1.1"}, probe: okProbe(100, "NRT")})
		for _, j := range drainJobs(d) {
			d.apply(jobResult{job: j, probe: okProbe(100, "NRT")})
		}
	}
	close(done)
}

// TestEstablishedConnSurvivesRoleChange: a PK/fill role swap must leave an
// already-established relay TCP connection fully functional.
func TestEstablishedConnSurvivesRoleChange(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			go io.Copy(c, c) // echo until closed
		}
	}()
	_, port, _ := net.SplitHostPort(up.Addr().String())
	d := testDaemon(t)
	fmt.Sscanf(port, "%d", &d.cfg.RelayPort)
	host := "127.0.0.1"
	d.pool.ips[host] = &IP{Addr: host, Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.updateRelaySnap()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go d.handleRelayConn(c)
		}
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ping := func(tag string) {
		t.Helper()
		cli.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := cli.Write([]byte(tag)); err != nil {
			t.Fatalf("%s write: %v", tag, err)
		}
		buf := make([]byte, len(tag))
		if _, err := io.ReadFull(cli, buf); err != nil || string(buf) != tag {
			t.Fatalf("%s echo: %v %q", tag, err, buf)
		}
	}
	ping("before")
	// Role replacement: the active leaves the pool entirely (hard-fail fill).
	d.pool.remove(host)
	d.pool.ips["127.0.0.2"] = &IP{Addr: "127.0.0.2", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.updateRelaySnap()
	ping("after") // established connection must keep flowing
}

func TestRelayConnectionsAreNotSyntheticProbeUsage(t *testing.T) {
	d := testDaemon(t)
	ip := &IP{Addr: "198.18.0.1", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	d.pool.ips[ip.Addr] = ip
	d.apply(jobResult{job: job{kind: jRelay, ip: ip.Addr}, probe: okProbe(10, "")})
	if len(d.dayCounts) != 0 {
		t.Fatal("customer TCP connection must not consume synthetic probe counters")
	}
	if d.relayStat(ip.Addr).Total != 1 {
		t.Fatal("real connection accounting must remain intact")
	}
}

// TestRelayWeightsUseProbeMedian: every active is weighted by the same probe
// metric; TCP dial time from relay traffic is a different unit.
func TestRelayWeightsUseProbeMedian(t *testing.T) {
	d := testDaemon(t)
	for _, addr := range []string{"198.18.0.1", "198.18.1.1"} {
		d.pool.ips[addr] = sinActive(addr, 300)
	}
	d.relayStat("198.18.0.1").EMAms = 100 // TCP dial time from real relay traffic
	d.updateRelaySnap()
	ts := d.relaySnap.Load().([]relayTarget)
	if len(ts) != 2 || ts[0].Weight != ts[1].Weight {
		t.Fatalf("equal probe medians must give equal weights, got %+v", ts)
	}
}

// TestRelayWeightsUseScore: relay weights follow the score
// rtt^-(1-w) * Mbps^w. An active over the latency floor (1.3x the best
// active) keeps serving at a quarter weight, like one under the bandwidth
// floor. Speeds and medians are fixtures.
func TestRelayWeightsUseScore(t *testing.T) {
	weights := func(w float64, actives ...*IP) map[string]float64 {
		d := testDaemon(t)
		d.cfg.BandwidthWeight = w
		for _, a := range actives {
			d.pool.ips[a.Addr] = a
		}
		d.updateRelaySnap()
		out := map[string]float64{}
		for _, t := range d.relaySnap.Load().([]relayTarget) {
			out[t.Addr] = t.Weight
		}
		return out
	}
	near := func(got, want float64) bool { return math.Abs(got/want-1) < 1e-9 }
	fast, slow := sinActive("198.18.0.1", 300), sinActive("198.18.1.1", 300)
	fast.Speed, slow.Speed = 300, 100
	if ws := weights(0.4, fast, slow); !near(ws[fast.Addr]/ws[slow.Addr], math.Pow(3, 0.4)) {
		t.Errorf("w=0.4: weight ratio %v, want 3^0.4", ws[fast.Addr]/ws[slow.Addr])
	}
	if ws := weights(0, fast, slow); ws[fast.Addr] != ws[slow.Addr] {
		t.Errorf("w=0: bandwidth must not change weights, got %v", ws)
	}
	far := sinActive("198.18.2.1", 450) // over 1.3 x 300 ms
	far.Speed = 300
	if ws := weights(0.4, fast, far); !near(ws[far.Addr]/ws[fast.Addr], math.Pow(450.0/300, -0.6)/4) {
		t.Errorf("an active over the latency floor must keep a quarter of its score, ratio %v", ws[far.Addr]/ws[fast.Addr])
	}
}

// TestRelayFeedbackReachesPromotedActive: a relay dial result must carry the
// generation of the active it dialed; a real active has a nonzero generation,
// so a zero would drop every real connection as stale.
func TestRelayFeedbackReachesPromotedActive(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(up.Addr().String())
	d := testDaemon(t)
	fmt.Sscanf(port, "%d", &d.cfg.RelayPort)
	ip := &IP{Addr: "127.0.0.1", Role: RoleStandby, Colo: "NRT"}
	d.pool.ips[ip.Addr] = ip
	d.pool.promoteActive(ip, 100, t0)
	d.updateRelaySnap()
	cli, srv := net.Pipe()
	defer cli.Close()
	go d.handleRelayConn(srv)
	select {
	case r := <-d.results:
		d.apply(r)
	case <-time.After(2 * time.Second):
		t.Fatal("relay dial result not reported")
	}
	if st := d.relayStat(ip.Addr); st.Total != 1 || st.EMAms <= 0 {
		t.Fatalf("relay feedback must reach a promoted active, got total=%d ema=%.1f", st.Total, st.EMAms)
	}
}

// TestCopyCountedUsesPooledBuffer: relay copies reuse a pooled 16 KiB buffer
// (one full TLS record) instead of allocating 32 KiB per direction per
// connection, and count bytes.
func TestCopyCountedUsesPooledBuffer(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64<<10)
	r := bytes.NewReader(payload)
	var n atomic.Int64
	if got := len(*relayBufs.Get().(*[]byte)); got != 16<<10 {
		t.Fatalf("relay buffer is %d bytes, want 16 KiB (one full TLS record)", got)
	}
	copyCounted(io.Discard, r, &n) // warm the pool
	const runs = 64
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	for range runs {
		r.Reset(payload)
		copyCounted(io.Discard, r, &n)
	}
	runtime.ReadMemStats(&b)
	if per := (b.TotalAlloc - a.TotalAlloc) / runs; per > 16<<10 {
		t.Fatalf("each copy allocated %d bytes; the buffer must come from the pool", per)
	}
	if got := n.Load(); got != int64(runs+1)*int64(len(payload)) {
		t.Fatalf("counted %d bytes", got)
	}
}

// relayVia starts a relay for d in front of an echo upstream on 127.0.0.1 and
// returns the relay address. With active false, the pool has no active.
func relayVia(t *testing.T, d *daemon, active bool) string {
	return relayTo(t, d, active, func(c net.Conn) { io.Copy(c, c); c.Close() })
}

// relayTo is relayVia with an upstream that serves each connection with serve.
func relayTo(t *testing.T, d *daemon, active bool, serve func(net.Conn)) string {
	t.Helper()
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { up.Close() })
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			go serve(c)
		}
	}()
	_, port, _ := net.SplitHostPort(up.Addr().String())
	fmt.Sscanf(port, "%d", &d.cfg.RelayPort)
	if active {
		d.pool.ips["127.0.0.1"] = &IP{Addr: "127.0.0.1", Role: RoleActive, State: StateHealthy, Colo: "NRT"}
	}
	d.updateRelaySnap()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go d.handleRelayConn(c)
		}
	}()
	return ln.Addr().String()
}

// waitClient polls the client table until cond holds for the only client.
func waitClient(t *testing.T, d *daemon, cond func(clientView) bool) clientView {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		cs := d.clients.list()
		if len(cs) == 1 && cond(cs[0]) {
			return cs[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("client table: %+v", cs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A relay connection is counted under its client IP: open while it lasts,
// with the bytes that it moved, and closed afterwards.
func TestRelayCountsClients(t *testing.T) {
	d := testDaemon(t)
	cli, err := net.Dial("tcp", relayVia(t, d, true))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetDeadline(time.Now().Add(2 * time.Second))
	cli.Write([]byte("hello"))
	if _, err := io.ReadFull(cli, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	// The up counter is added after the write returns, so the echo can be
	// counted first: wait for both.
	c := waitClient(t, d, func(c clientView) bool { return c.BytesUp == 5 && c.BytesDown == 5 })
	if c.Addr != "127.0.0.1" || c.Total != 1 || c.Open != 1 || c.BytesUp != 5 || c.Fails != 0 || c.Last == "" {
		t.Fatalf("open connection: %+v", c)
	}
	cli.Close()
	waitClient(t, d, func(c clientView) bool { return c.Open == 0 && c.Total == 1 })
}

// With no active to dial, the client still shows up, with a failure.
func TestRelayCountsClientFailures(t *testing.T) {
	d := testDaemon(t)
	cli, err := net.Dial("tcp", relayVia(t, d, false))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitClient(t, d, func(c clientView) bool { return c.Total == 1 && c.Fails == 1 && c.Open == 0 })
}

// The relay port can face the internet, so the table is bounded: a new client
// replaces the idle client seen longest ago, never a busy one.
func TestClientTableEvictsIdleOldest(t *testing.T) {
	var ct clientTable
	busy := ct.open("busy", t0)
	for i := 1; i < maxClients; i++ {
		at := t0.Add(time.Duration(i) * time.Second)
		ct.done(ct.open(fmt.Sprint("idle", i), at), false, at)
	}
	ct.open("new", t0.Add(time.Hour))
	seen := map[string]bool{}
	for _, c := range ct.list() {
		seen[c.Addr] = true
	}
	if len(seen) != maxClients || !seen["busy"] || !seen["new"] || seen["idle1"] || !seen["idle2"] {
		t.Fatalf("after eviction: %d clients, busy=%v new=%v idle1=%v idle2=%v",
			len(seen), seen["busy"], seen["new"], seen["idle1"], seen["idle2"])
	}
	ct.done(busy, false, t0.Add(time.Hour))
}

// A client is last seen when its latest connection opened or closed, so a
// long connection that just ended does not show as hours old.
func TestClientLastSeenAtClose(t *testing.T) {
	var ct clientTable
	ct.done(ct.open("192.0.2.1", t0), false, t0.Add(3*time.Hour))
	if c := ct.list()[0]; c.Last != t0.Add(3*time.Hour).Format(time.RFC3339) {
		t.Fatalf("last seen %s, want the close time", c.Last)
	}
}

// Each relay connection is listed while open, with its client address,
// upstream active, and bytes, and stays listed after it closes.
func TestRelayListsConnections(t *testing.T) {
	d := testDaemon(t)
	cli, err := net.Dial("tcp", relayVia(t, d, true))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetDeadline(time.Now().Add(2 * time.Second))
	cli.Write([]byte("hello"))
	if _, err := io.ReadFull(cli, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	waitConn := func(cond func(connView) bool) connView {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if cs := d.conns.list(); len(cs) == 1 && cond(cs[0]) {
				return cs[0]
			}
			if time.Now().After(deadline) {
				t.Fatalf("connections: %+v", d.conns.list())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	c := waitConn(func(c connView) bool { return c.BytesUp == 5 && c.BytesDown == 5 })
	if c.Client != cli.LocalAddr().String() || c.Upstream != "127.0.0.1" || c.BytesUp != 5 || c.End != "" || c.Err != "" {
		t.Fatalf("open connection: %+v", c)
	}
	cli.Close()
	waitConn(func(c connView) bool { return c.End != "" && c.BytesDown == 5 && c.Err == "" })
}

// A connection that finds no active is listed as failed, with the reason.
func TestRelayListsFailedConnections(t *testing.T) {
	d := testDaemon(t)
	cli, err := net.Dial("tcp", relayVia(t, d, false))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cs := d.conns.list(); len(cs) == 1 && cs[0].End != "" && cs[0].Err != "" && cs[0].Upstream == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections: %+v", d.conns.list())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Closed connections are kept in a bounded ring, newest first; open ones are
// always listed, before the closed ones.
func TestConnLogKeepsRecentClosed(t *testing.T) {
	var l connLog
	busy := l.start("192.0.2.1:1000", t0)
	for i := range recentConns + 50 {
		at := t0.Add(time.Duration(i+1) * time.Second)
		l.end(l.start(fmt.Sprintf("192.0.2.2:%d", i), at), "", at)
	}
	cs := l.list()
	if len(cs) != recentConns+1 || cs[0].Client != busy.client || cs[0].End != "" {
		t.Fatalf("got %d entries, first %+v", len(cs), cs[0])
	}
	if want := fmt.Sprintf("192.0.2.2:%d", recentConns+49); cs[1].Client != want {
		t.Fatalf("newest closed first: got %s, want %s", cs[1].Client, want)
	}
}

// clientHello returns the first bytes that a real TLS client sends for name.
func clientHello(t *testing.T, name string) []byte {
	t.Helper()
	cli, srv := net.Pipe()
	defer srv.Close()
	go tls.Client(cli, &tls.Config{ServerName: name}).Handshake()
	buf := make([]byte, 0, 4096)
	srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		chunk := make([]byte, 4096)
		n, err := srv.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if len(buf) >= 5 && len(buf) >= 5+int(buf[3])<<8|int(buf[4]) || err != nil {
			cli.Close()
			return buf
		}
	}
}

// The SNI parser reads the name from a real ClientHello, also when the hello
// arrives in pieces, and gives up on data that is not TLS.
func TestClientHelloSNI(t *testing.T) {
	hello := clientHello(t, "example.com")
	if name, done := clientHelloSNI(hello); !done || name != "example.com" {
		t.Fatalf("whole hello: %q %v", name, done)
	}
	if _, done := clientHelloSNI(hello[:len(hello)/2]); done {
		t.Fatal("half a hello must ask for more data")
	}
	var got string
	s := &sniSniffer{r: io.MultiReader(bytes.NewReader(hello[:3]), bytes.NewReader(hello[3:700]), bytes.NewReader(hello[700:])),
		found: func(n string) { got = n }}
	if b, _ := io.ReadAll(s); !bytes.Equal(b, hello) || got != "example.com" {
		t.Fatalf("sniffer must pass bytes through unchanged and find the name, got %q", got)
	}
	if name, done := clientHelloSNI([]byte("GET / HTTP/1.1\r\n")); !done || name != "" {
		t.Fatalf("plain HTTP: %q %v", name, done)
	}
}

// A relayed TLS connection is listed with the SNI of its ClientHello.
func TestRelayListsSNI(t *testing.T) {
	d := testDaemon(t)
	cli, err := net.Dial("tcp", relayVia(t, d, true))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.SetDeadline(time.Now().Add(2 * time.Second))
	go tls.Client(cli, &tls.Config{ServerName: "example.com"}).Handshake() // the echo upstream fails it
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cs := d.conns.list(); len(cs) == 1 && cs[0].SNI == "example.com" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections: %+v", d.conns.list())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A client that half-closes after its request still gets the whole reply, also
// when the upstream answers only after it reads the end of the request.
func TestRelayKeepsReplyAfterClientHalfClose(t *testing.T) {
	d := testDaemon(t)
	addr := relayTo(t, d, true, func(c net.Conn) {
		defer c.Close()
		io.Copy(io.Discard, c)
		time.Sleep(300 * time.Millisecond)
		c.Write([]byte("reply"))
	})
	cli, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.SetDeadline(time.Now().Add(3 * time.Second))
	cli.Write([]byte("request"))
	cli.(*net.TCPConn).CloseWrite()
	if b, err := io.ReadAll(cli); string(b) != "reply" {
		t.Fatalf("got %q, %v; want the reply after the half-close", b, err)
	}
}

// A half-closed connection whose upstream neither answers nor closes ends once
// the remaining direction has been quiet for halfCloseIdle.
func TestRelayEndsQuietHalfClosedConnection(t *testing.T) {
	d := testDaemon(t)
	d.halfCloseIdle = 200 * time.Millisecond
	quit := make(chan struct{})
	defer close(quit)
	addr := relayTo(t, d, true, func(c net.Conn) {
		defer c.Close()
		io.Copy(io.Discard, c)
		<-quit // read the end of the request, then hold the connection silently
	})
	cli, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.Write([]byte("request"))
	cli.(*net.TCPConn).CloseWrite()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cs := d.conns.list(); len(cs) == 1 && cs[0].End != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a quiet half-closed connection must end: %+v", d.conns.list())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A client that half-closes and then stops reading cannot pin a streaming
// upstream: writes have halfCloseIdle to complete, like reads.
func TestRelayEndsHalfClosedConnectionThatStopsReading(t *testing.T) {
	d := testDaemon(t)
	d.halfCloseIdle = 200 * time.Millisecond
	addr := relayTo(t, d, true, func(c net.Conn) {
		defer c.Close()
		io.Copy(io.Discard, c)
		chunk := make([]byte, 64<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				return
			}
		}
	})
	cli, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	cli.Write([]byte("request"))
	cli.(*net.TCPConn).CloseWrite() // and never read
	deadline := time.Now().Add(3 * time.Second)
	for {
		if cs := d.conns.list(); len(cs) == 1 && cs[0].End != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a half-closed client that stops reading must not pin the connection: %+v", d.conns.list())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
