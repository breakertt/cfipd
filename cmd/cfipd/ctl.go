package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"sort"
	"text/tabwriter"
	"time"
)

type ctlRequest struct {
	path  string
	arg   string
	reply chan ctlReply
}

type ctlReply struct {
	v   any
	err string
}

func serveCtl(d *daemon) error {
	os.Remove(d.cfg.Socket)
	ln, err := net.Listen("unix", d.cfg.Socket)
	if err != nil {
		return fmt.Errorf("listen %s: %w", d.cfg.Socket, err)
	}
	mux := http.NewServeMux()
	for _, p := range []string{"/status", "/actives", "/standbys", "/candidates", "/prefixes", "/promote", "/quarantine", "/scan", "/relay", "/add"} {
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			req := ctlRequest{path: r.URL.Path, arg: r.URL.Query().Get("ip"), reply: make(chan ctlReply, 1)}
			select {
			case d.ctl <- req:
			case <-d.ctx.Done():
				http.Error(w, "daemon shutting down", http.StatusServiceUnavailable)
				return
			}
			rep := <-req.reply
			if rep.err != "" {
				http.Error(w, rep.err, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(rep.v)
		})
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	go func() { <-d.ctx.Done(); srv.Close() }()
	return nil
}

type ipView struct {
	Addr    string  `json:"addr"`
	Role    Role    `json:"role"`
	State   string  `json:"state,omitempty"`
	Colo    string  `json:"colo,omitempty"`
	Median  float64 `json:"median_ms,omitempty"` // HTTP probe duration, not pure RTT
	Jitter  float64 `json:"jitter_ms,omitempty"`
	Samples int     `json:"samples"`
	Fail    float64 `json:"fail_rate,omitempty"` // probe failure rate; absent when unsampled
	WinAge  string  `json:"win_age,omitempty"`   // age of oldest retained sample
	Speed   float64 `json:"speed_mbps,omitempty"`
	SpeedAt string  `json:"speed_at,omitempty"` // unknown for pre-v2 values
	Every   string  `json:"probe_every,omitempty"`
	NextIn  string  `json:"next_in,omitempty"`
	CoolIn  string  `json:"cooldown_in,omitempty"`
}

func view(cfg *Config, ip *IP, now time.Time) ipView {
	v := ipView{
		Addr:    ip.Addr,
		Role:    ip.Role,
		State:   ip.State,
		Colo:    ip.Colo,
		Median:  ip.medianRTT(),
		Jitter:  ip.jitter(),
		Samples: len(ip.Win),
		Speed:   ip.Speed,
	}
	if rate, ok := ip.successRate(); ok {
		v.Fail = 1 - rate
	}
	if ws := ip.windowStart(); !ws.IsZero() {
		v.WinAge = now.Sub(ws).Round(time.Second).String()
	}
	if !ip.SpeedAt.IsZero() {
		v.SpeedAt = ip.SpeedAt.Format(time.RFC3339)
	} else if ip.Speed > 0 {
		v.SpeedAt = "unknown" // historical value carried over from v1
	}
	if !ip.NextProbe.IsZero() {
		if d := ip.NextProbe.Sub(now); d > 0 {
			v.NextIn = d.Round(time.Second).String()
		} else {
			v.NextIn = "due"
		}
	}
	if ip.CooldownUntil.After(now) {
		v.CoolIn = ip.CooldownUntil.Sub(now).Round(time.Second).String()
	}
	if ip.Role == RoleActive {
		v.Every = ip.healthEvery(cfg).String()
	}
	return v
}

func views(cfg *Config, ips []*IP, now time.Time) []ipView {
	out := make([]ipView, 0, len(ips))
	for _, ip := range ips {
		out = append(out, view(cfg, ip, now))
	}
	return out
}

func (d *daemon) handleCtl(req ctlRequest) {
	now := time.Now()
	rep := ctlReply{}
	switch req.path {
	case "/status":
		pk := map[string]any{
			"running":       d.pkRunning,
			"bw_used_today": d.pool.pk.BWUsed,
		}
		if d.pkLastVerdict != "" {
			pk["last"] = d.pkLastVerdict
		}
		if !d.pool.pk.ObserveUntil.IsZero() && now.Before(d.pool.pk.ObserveUntil) {
			pk["observe_until"] = d.pool.pk.ObserveUntil.Format(time.RFC3339)
		}
		if d.pkPending != nil {
			pk["pending"] = d.pkPending.Challenger + " vs " + d.pkPending.Incumbent
		}
		rep.v = map[string]any{
			"uptime":       now.Sub(d.started).Round(time.Second).String(),
			"actives":      views(d.cfg, d.pool.actives(), now),
			"standbys":     len(d.pool.standbys()),
			"candidates":   len(d.pool.candidates()),
			"scan_every":   d.scanInterval().String(),
			"promoting":    d.promoting,
			"pk":           pk,
			"probes_today": d.dayCounts,
			"bytes_today":  d.pool.dayBytes,
		}
	case "/actives":
		rep.v = views(d.cfg, d.pool.actives(), now)
	case "/standbys":
		rep.v = views(d.cfg, d.pool.rankedStandbys(), now)
	case "/candidates":
		cands := d.pool.candidates()
		sort.Slice(cands, func(i, j int) bool { return cands[i].LastSeen.After(cands[j].LastSeen) })
		if len(cands) > 50 {
			cands = cands[:50]
		}
		rep.v = views(d.cfg, cands, now)
	case "/prefixes":
		stats := make([]*PrefixStat, 0, len(d.scan.stats))
		for _, st := range d.scan.stats {
			stats = append(stats, st)
		}
		sort.Slice(stats, func(i, j int) bool { return stats[i].Probes > stats[j].Probes })
		if len(stats) > 50 {
			stats = stats[:50]
		}
		rep.v = stats
	case "/promote":
		ip := d.pool.get(req.arg)
		if ip == nil {
			rep.err = "unknown ip"
		} else if why := d.longJobBlocked(now); why != "" {
			rep.err = why
		} else if ip.Role != RoleStandby {
			rep.err = "only a standby can be promoted"
		} else if len(d.pool.actives()) >= d.cfg.ActiveCount {
			rep.err = "actives full; elective replacement goes through PK, not manual promote"
		} else if !d.pool.diverseWith(ip, d.pool.actives(), false) {
			rep.err = "promotion would violate prefix/colo diversity"
		} else {
			d.startPromotion(ip, job{})
			rep.v = map[string]string{"msg": "promotion test started for " + ip.Addr}
		}
	case "/quarantine":
		ip := d.pool.get(req.arg)
		if ip == nil {
			rep.err = "unknown ip"
		} else {
			d.pool.cooldown(ip, "manual", now)
			rep.v = map[string]string{"msg": ip.Addr + " cooled down until " + ip.CooldownUntil.Format(time.RFC3339)}
		}
	case "/scan":
		d.nextScan = now
		rep.v = map[string]string{"msg": "scan scheduled"}
	case "/add":
		a, err := netip.ParseAddr(req.arg)
		if err != nil {
			rep.err = "bad ip: " + req.arg
			break
		}
		addr := a.String()
		if d.pool.get(addr) != nil {
			rep.v = "exists"
			break
		}
		ip := d.pool.ensure(addr, now)
		ip.NextProbe = now
		rep.v = "added"
	case "/relay":
		type rs struct {
			Addr      string  `json:"addr"`
			Total     int64   `json:"total"`
			Fails     int64   `json:"fails"`
			EMAms     float64 `json:"connect_ema_ms"`
			BytesUp   int64   `json:"bytes_up"`
			BytesDown int64   `json:"bytes_down"`
			Conns     int64   `json:"conns"`
		}
		var out []rs
		for addr, st := range d.relayStats {
			out = append(out, rs{addr, st.Total, st.Fails, st.EMAms, st.BytesUp.Load(), st.BytesDown.Load(), st.Conns.Load()})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Total > out[j].Total })
		rep.v = out
	default:
		rep.err = "unknown path"
	}
	req.reply <- rep
}

func ctlGet(sock, path, ip string) (json.RawMessage, error) {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", sock)
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	u := "http://unix" + path
	if ip != "" {
		u += "?ip=" + url.QueryEscape(ip)
	}
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", b)
	}
	return b, nil
}

func printTable(raw json.RawMessage) error {
	var vs []ipView
	if err := json.Unmarshal(raw, &vs); err != nil {
		fmt.Printf("%s", raw)
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "IP\tROLE\tSTATE\tCOLO\tHTTP(ms)\tJIT(ms)\tFAIL%\tSAMPLES\tNEXT-IN\tCOOL-IN")
	for _, v := range vs {
		fail := "—"
		if v.Samples > 0 {
			fail = fmt.Sprintf("%.0f%%", v.Fail*100)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.0f\t%.0f\t%s\t%d\t%s\t%s\n",
			v.Addr, v.Role, v.State, v.Colo, v.Median, v.Jitter, fail, v.Samples, v.NextIn, v.CoolIn)
	}
	return w.Flush()
}
