package main

import (
	"os"
	"testing"
)

// TestFindConfigPrecedence: $CFIPD_CONFIG_PATH beats ./cfipd.json, which
// beats /etc/cfipd.json; -config (not tested here) beats all of them.
func TestFindConfigPrecedence(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("cfipd.json", []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CFIPD_CONFIG_PATH", "")
	if got := findConfig(); got != "cfipd.json" {
		t.Fatalf("without the variable, ./cfipd.json must win, got %q", got)
	}
	t.Setenv("CFIPD_CONFIG_PATH", "/srv/cfipd/other.json")
	if got := findConfig(); got != "/srv/cfipd/other.json" {
		t.Fatalf("CFIPD_CONFIG_PATH must win over ./cfipd.json, got %q", got)
	}
}
