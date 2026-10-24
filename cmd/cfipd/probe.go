package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ProbeResult struct {
	OK   bool
	Hard bool // TLS/cert/refused: this IP will never serve our SNI
	// Restricted: the IP answers error 1034 for ZoneProbeHost (implies Hard);
	// its whole /24 is learned as bad.
	Restricted bool
	Err        string
	TCPms      float64
	TLSms      float64
	TTFBms     float64
	Totalms    float64
	Colo       string
	T          time.Time // sample start time, truthful per physical request
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func dialFixed(ip string, timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		return d.DialContext(ctx, network, net.JoinHostPort(ip, port))
	}
}

func classifyErr(err error) bool {
	var unk x509.UnknownAuthorityError
	var host x509.HostnameError
	var inv x509.CertificateInvalidError
	var rec tls.RecordHeaderError
	if errors.As(err, &unk) || errors.As(err, &host) || errors.As(err, &inv) || errors.As(err, &rec) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && strings.Contains(err.Error(), "refused") {
		return true
	}
	return false
}

func transport(ip, sniHost string) *http.Transport {
	return &http.Transport{
		DialContext:           dialFixed(ip, 4*time.Second),
		TLSClientConfig:       &tls.Config{ServerName: sniHost},
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 5 * time.Second,
	}
}

func parseColo(body string) string {
	for _, line := range strings.Split(body, "\n") {
		if v, ok := strings.CutPrefix(line, "colo="); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func coloFromRay(ray string) string {
	if i := strings.LastIndex(ray, "-"); i >= 0 {
		return ray[i+1:]
	}
	return ""
}

func probe(ctx context.Context, cfg *Config, ip string) ProbeResult {
	var res ProbeResult
	u, err := url.Parse(cfg.TraceURL)
	if err != nil {
		res.Err = err.Error()
		return res
	}

	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()

	t0 := time.Now()
	res.T = t0
	var tcpDone, tlsDone, ttfb time.Time
	trace := &httptrace.ClientTrace{
		ConnectDone:          func(_, _ string, _ error) { tcpDone = time.Now() },
		TLSHandshakeDone:     func(_ tls.ConnectionState, _ error) { tlsDone = time.Now() },
		GotFirstResponseByte: func() { ttfb = time.Now() },
	}

	tr := transport(ip, u.Hostname())
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, cfg.TraceURL, nil)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	req.Header.Set("User-Agent", "cfipd/0.1")
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		res.Err = err.Error()
		res.Hard = classifyErr(err)
		return res
	}
	defer resp.Body.Close()
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	res.Totalms = ms(time.Since(t0))
	if !tcpDone.IsZero() {
		res.TCPms = ms(tcpDone.Sub(t0))
	}
	if !tlsDone.IsZero() && !tcpDone.IsZero() {
		res.TLSms = ms(tlsDone.Sub(tcpDone))
	}
	if !ttfb.IsZero() {
		res.TTFBms = ms(ttfb.Sub(t0))
	}
	if rerr != nil {
		res.Err = rerr.Error()
		return res
	}
	if resp.StatusCode != http.StatusOK {
		res.Err = fmt.Sprintf("status %d", resp.StatusCode)
		return res
	}
	res.Colo = parseColo(string(body))
	if res.Colo == "" {
		res.Colo = coloFromRay(resp.Header.Get("CF-RAY"))
	}
	res.OK = res.Colo != ""
	if !res.OK {
		res.Err = "no colo in trace"
		return res
	}
	// Only a colo we would accept is worth the second request.
	if cfg.ZoneProbeHost != "" && cfg.coloAllowed(res.Colo) {
		restricted, err := zoneRestricted(ctx, cfg, ip)
		switch {
		case restricted:
			res.OK, res.Hard, res.Restricted = false, true, true
			res.Err = "zone " + cfg.ZoneProbeHost + ": error 1034 (edge IP restricted)"
		case err != nil:
			res.OK, res.Err = false, "zone probe: "+err.Error()
		}
	}
	return res
}

// zoneRestricted GETs / on cfg.ZoneProbeHost through ip (SNI and Host = the
// zone host) and reports whether Cloudflare answered error code 1034.
// Anything else (404 from the origin, 5xx, ...) is not an IP problem.
func zoneRestricted(ctx context.Context, cfg *Config, ip string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tr := transport(ip, cfg.ZoneProbeHost)
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+cfg.ZoneProbeHost+"/", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "cfipd/0.1")
	req.Close = true
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil && len(body) == 0 {
		return false, err
	}
	return strings.Contains(string(body), "error code: 1034"), nil
}

type SpeedResult struct {
	Status int // HTTP status of the download response; 0 without a response
	Mbps   float64
	Bytes  int64
	Dur    time.Duration
	Colo   string
	Err    string
	Hard   bool
}

func speedTest(ctx context.Context, cfg *Config, ip string) SpeedResult {
	var res SpeedResult
	u, err := url.Parse(cfg.DownURL)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	q := u.Query()
	q.Set("bytes", strconv.FormatInt(cfg.SpeedBytes, 10))
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(ctx, cfg.SpeedTimeout.Duration)
	defer cancel()

	tr := transport(ip, u.Hostname())
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		res.Err = err.Error()
		return res
	}
	req.Header.Set("User-Agent", "cfipd/0.1")
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		res.Err = err.Error()
		res.Hard = classifyErr(err)
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		res.Err = fmt.Sprintf("status %d", resp.StatusCode)
		return res
	}
	res.Colo = coloFromRay(resp.Header.Get("CF-RAY"))

	start := time.Now()
	n, rerr := io.Copy(io.Discard, resp.Body)
	res.Bytes = n
	res.Dur = time.Since(start)
	if res.Dur > 0 {
		res.Mbps = float64(n) * 8 / res.Dur.Seconds() / 1e6
	}
	if rerr != nil && !errors.Is(rerr, context.DeadlineExceeded) {
		res.Err = rerr.Error()
	}
	return res
}
