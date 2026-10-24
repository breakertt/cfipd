package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
