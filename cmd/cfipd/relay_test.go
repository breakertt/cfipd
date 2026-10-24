package main

import (
	"context"
	"fmt"
	"io"
	"net"
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
