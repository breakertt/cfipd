package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"
)

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type Config struct {
	DataDir string `json:"data_dir"`
	Socket  string `json:"socket"`
	// WebPassword, if set, protects `cfipd web` with HTTP Basic auth; the
	// user name is ignored.
	WebPassword string `json:"web_password"`

	TraceURL string `json:"trace_url"`
	DownURL  string `json:"down_url"`
	// ZoneProbeHost, if set, is a plain orange-cloud (proxied A record)
	// hostname; every probe that passes the trace also GETs / on it via the
	// candidate IP (SNI+Host) and an IP answering Cloudflare error 1034 (Edge
	// IP Restricted) fails hard. The trace host works on every CF IP and
	// cannot see this. Empty = off.
	ZoneProbeHost string `json:"zone_probe_host"`

	AllowedColos []string `json:"allowed_colos"`

	ActiveCount    int      `json:"active_count"`
	HealthInterval Duration `json:"health_interval"` // base tier; actives back off up to 12x
	WindowSize     int      `json:"window_size"`
	DegradedFails  int      `json:"degraded_fails"`
	DownFails      int      `json:"down_fails"`
	RecoverOKs     int      `json:"recover_oks"`
	HardFailDown   int      `json:"hard_fail_down"`
	ColoBadDown    int      `json:"colo_bad_down"`

	StandbyMin    int      `json:"standby_min"`
	StandbyTarget int      `json:"standby_target"`
	StandbyMax    int      `json:"standby_max"`
	StandbyCheck  Duration `json:"standby_check"` // hot tier: best standbys
	StandbyCold   Duration `json:"standby_cold"`  // cold tier: remaining standbys

	ScanBootstrap Duration `json:"scan_bootstrap"`
	ScanLow       Duration `json:"scan_low"`    // backup deficit
	ScanStable    Duration `json:"scan_stable"` // stable base, backs off toward ScanMax on no yield
	ScanMax       Duration `json:"scan_max"`
	ScanV6Ratio   float64  `json:"scan_v6_ratio"`

	ValidationObs    int      `json:"validation_obs"`
	ValidationGap    Duration `json:"validation_gap"`
	ValidationWindow Duration `json:"validation_window"` // max candidate lifetime

	PromoteProbes    int      `json:"promote_probes"`
	PromoteInterval  Duration `json:"promote_interval"`
	PromoteMaxFails  int      `json:"promote_max_fails"`
	PromoteMaxJitter float64  `json:"promote_max_jitter_ms"`
	PromoteMinMbps   float64  `json:"promote_min_mbps"`
	// BandwidthWeight trades probe time against bandwidth in relay weights
	// and the PK bandwidth stage: 0 is latency only, 1 is bandwidth only.
	BandwidthWeight float64  `json:"bandwidth_weight"`
	SpeedBytes      int64    `json:"speed_bytes"`
	SpeedTimeout    Duration `json:"speed_timeout"`
	SpeedRecheck    Duration `json:"speed_recheck"` // re-measure each active this often; 0 disables
	SpeedGap        Duration `json:"speed_gap"`     // minimum time between bandwidth downloads

	MemoryLimitMB int `json:"memory_limit_mb"` // Go soft memory limit; 0 follows the host's free memory

	MaxActivePerPrefix int `json:"max_active_per_prefix24"`
	MaxActivePerColo   int `json:"max_active_per_colo"`

	Cooldown Duration `json:"cooldown"` // failed nodes rest in Candidate this long

	RelayListen string   `json:"relay_listen"`
	RelayPort   int      `json:"relay_port"`
	RelayDial   Duration `json:"relay_dial_timeout"`

	Workers      int `json:"workers"`
	CandidateCap int `json:"candidate_cap"`
	// CandidatePerPrefix bounds candidates per /24 (v4) or /48 (v6).
	CandidatePerPrefix int `json:"candidate_per_prefix"`
}

func defaultConfig() *Config {
	return &Config{
		DataDir:  "/var/lib/cfipd",
		Socket:   "/tmp/cfipd.sock",
		TraceURL: "https://speed.cloudflare.com/cdn-cgi/trace",
		DownURL:  "https://speed.cloudflare.com/__down",

		AllowedColos: []string{"NRT", "KIX", "HKG", "SIN"},

		ActiveCount:    3,
		HealthInterval: Duration{10 * time.Second},
		WindowSize:     12,
		DegradedFails:  2,
		DownFails:      5,
		RecoverOKs:     3,
		HardFailDown:   2,
		ColoBadDown:    2,

		StandbyMin:    2,
		StandbyTarget: 8,
		StandbyMax:    8,
		StandbyCheck:  Duration{5 * time.Minute},
		StandbyCold:   Duration{30 * time.Minute},

		ScanBootstrap: Duration{1 * time.Second},
		ScanLow:       Duration{5 * time.Second},
		ScanStable:    Duration{1 * time.Minute},
		ScanMax:       Duration{1 * time.Hour},
		ScanV6Ratio:   0.2,

		ValidationObs:    3,
		ValidationGap:    Duration{2 * time.Minute},
		ValidationWindow: Duration{2 * time.Hour},

		PromoteProbes:    8,
		PromoteInterval:  Duration{750 * time.Millisecond},
		PromoteMaxFails:  0,
		PromoteMaxJitter: 30,
		PromoteMinMbps:   30,
		BandwidthWeight:  0.4,
		SpeedBytes:       99_999_999,
		SpeedTimeout:     Duration{10 * time.Second},
		SpeedRecheck:     Duration{time.Hour}, // three actives: one recheck every ~20 min under the 15 min speed_gap
		SpeedGap:         Duration{15 * time.Minute},

		MaxActivePerPrefix: 1,
		MaxActivePerColo:   2,

		Cooldown: Duration{10 * time.Minute},

		RelayListen: ":8443",
		RelayPort:   443,
		RelayDial:   Duration{3 * time.Second},

		Workers:            12,
		CandidateCap:       256,
		CandidatePerPrefix: 2,
	}
}

// loadConfig overlays the JSON file at path on the built-in defaults; an
// empty path means defaults only. With strict (cfipd test), a missing file,
// an unknown key or an out-of-range value is an error. Otherwise a missing
// file means defaults and unknown keys are ignored, so a stale key never stops
// a running service; the daemon logs the strict result at startup instead.
func loadConfig(path string, strict bool) (*Config, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !strict {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if strict {
		dec.DisallowUnknownFields()
	}
	err = dec.Decode(cfg)
	if err == nil && dec.Decode(&json.RawMessage{}) != io.EOF {
		err = errors.New("trailing data after the JSON object")
	}
	if err == nil && strict {
		err = cfg.check()
	}
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// check reports the first setting that would keep the daemon from working.
func (c *Config) check() error {
	for _, v := range []struct {
		key string
		ok  bool
	}{
		{"data_dir", c.DataDir != ""},
		{"socket", c.Socket != ""},
		{"trace_url", validURL(c.TraceURL)},
		{"down_url", validURL(c.DownURL)},
		{"allowed_colos", len(c.AllowedColos) > 0},
		{"active_count", c.ActiveCount >= 1},
		{"health_interval", c.HealthInterval.Duration > 0},
		{"window_size", c.WindowSize >= 1},
		{"degraded_fails", c.DegradedFails >= 1},
		{"down_fails", c.DownFails >= 1},
		{"recover_oks", c.RecoverOKs >= 1},
		{"hard_fail_down", c.HardFailDown >= 1},
		{"colo_bad_down", c.ColoBadDown >= 1},
		{"standby_min/standby_target/standby_max", 0 <= c.StandbyMin && c.StandbyMin <= c.StandbyTarget && c.StandbyTarget <= c.StandbyMax},
		{"scan_v6_ratio", c.ScanV6Ratio >= 0 && c.ScanV6Ratio <= 1},
		{"validation_obs", c.ValidationObs >= 1},
		{"validation_window", c.ValidationWindow.Duration > 0},
		{"promote_probes", c.PromoteProbes >= 1},
		{"promote_max_fails", c.PromoteMaxFails >= 0 && c.PromoteMaxFails < c.PromoteProbes},
		{"promote_max_jitter_ms", c.PromoteMaxJitter >= 0},
		{"promote_min_mbps", c.PromoteMinMbps >= 0},
		{"bandwidth_weight", c.BandwidthWeight >= 0 && c.BandwidthWeight <= 1},
		{"speed_bytes", c.SpeedBytes >= 1},
		{"speed_timeout", c.SpeedTimeout.Duration > 0},
		{"max_active_per_prefix24", c.MaxActivePerPrefix >= 1},
		{"max_active_per_colo", c.MaxActivePerColo >= 1},
		{"relay_port", c.RelayPort >= 1 && c.RelayPort <= 65535},
		{"workers", c.Workers >= 1},
		{"candidate_cap", c.CandidateCap >= 1},
		{"candidate_per_prefix", c.CandidatePerPrefix >= 1},
		{"memory_limit_mb", c.MemoryLimitMB >= 0},
	} {
		if !v.ok {
			return fmt.Errorf("%s: value out of range", v.key)
		}
	}
	rv := reflect.ValueOf(c).Elem()
	for i := range rv.NumField() {
		if d, ok := rv.Field(i).Interface().(Duration); ok && d.Duration < 0 {
			key, _, _ := strings.Cut(rv.Type().Field(i).Tag.Get("json"), ",")
			return fmt.Errorf("%s: negative duration", key)
		}
	}
	return nil
}

func validURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func (c *Config) coloAllowed(colo string) bool {
	for _, a := range c.AllowedColos {
		if a == colo {
			return true
		}
	}
	return false
}
