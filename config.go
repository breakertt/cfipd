package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	SpeedBytes       int64    `json:"speed_bytes"`
	SpeedTimeout     Duration `json:"speed_timeout"`

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
		SpeedBytes:       99_999_999,
		SpeedTimeout:     Duration{10 * time.Second},

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

func loadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) coloAllowed(colo string) bool {
	for _, a := range c.AllowedColos {
		if a == colo {
			return true
		}
	}
	return false
}
