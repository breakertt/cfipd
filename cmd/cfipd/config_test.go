package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestLoadConfigDefaultsPassCheck: the built-in defaults are a valid
// configuration, and an empty path means defaults only.
func TestLoadConfigDefaultsPassCheck(t *testing.T) {
	cfg, err := loadConfig("", true)
	if err != nil || !reflect.DeepEqual(cfg, defaultConfig()) {
		t.Fatalf("empty path must give the defaults, got %+v, %v", cfg, err)
	}
	if err := defaultConfig().check(); err != nil {
		t.Fatalf("built-in defaults must pass the check: %v", err)
	}
}

// TestLoadConfigOverlaysDefaults: a file overrides only the keys it names.
func TestLoadConfigOverlaysDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfipd.json")
	body := `{"active_count": 2, "speed_gap": "30m", "allowed_colos": ["LAX"]}`
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(p, true)
	if err != nil {
		t.Fatal(err)
	}
	want := defaultConfig()
	want.ActiveCount, want.SpeedGap, want.AllowedColos = 2, Duration{30 * time.Minute}, []string{"LAX"}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("overlay mismatch:\n got %+v\nwant %+v", cfg, want)
	}
}

// TestConfigTestRejectsBadFiles: the strict check behind `cfipd test` reports
// every class of mistake, while the daemon's lenient load tolerates a stale
// key, an out-of-range value it can still run with, and a missing file.
func TestConfigTestRejectsBadFiles(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name, body, want string
		lenientOK        bool
	}{
		{"unknown key", `{"speed_gapp": "30m"}`, `unknown field "speed_gapp"`, true},
		{"malformed", `{"active_count": 3,}`, "invalid character", false},
		{"bad duration", `{"speed_gap": "15 minutes"}`, "unknown unit", false},
		{"trailing data", `{} {}`, "trailing data", false},
		{"zero actives", `{"active_count": 0}`, "active_count", true},
		{"v6 ratio", `{"scan_v6_ratio": 1.5}`, "scan_v6_ratio", true},
		{"max fails", `{"promote_max_fails": 8}`, "promote_max_fails", true},
		{"negative gap", `{"speed_gap": "-1m"}`, "speed_gap", true},
		{"no colos", `{"allowed_colos": []}`, "allowed_colos", true},
		{"bad url", `{"down_url": "speed.cloudflare.com/__down"}`, "down_url", true},
	} {
		p := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "-")+".json")
		if err := os.WriteFile(p, []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(p, true); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: strict error %v, want one containing %q", tc.name, err, tc.want)
		}
		if _, err := loadConfig(p, false); (err == nil) != tc.lenientOK {
			t.Errorf("%s: lenient load error %v, want ok=%v", tc.name, err, tc.lenientOK)
		}
	}
	missing := filepath.Join(dir, "missing.json")
	if _, err := loadConfig(missing, true); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("strict load of a missing file must fail, got %v", err)
	}
	if cfg, err := loadConfig(missing, false); err != nil || !reflect.DeepEqual(cfg, defaultConfig()) {
		t.Errorf("lenient load of a missing file must give the defaults, got %v", err)
	}
}
