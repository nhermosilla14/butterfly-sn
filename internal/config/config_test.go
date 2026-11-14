package config

import (
	"log/slog"
	"strings"
	"testing"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(strings.NewReader("listen: 0.0.0.0:5684\nbroker: 127.0.0.1:1883\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxClients != 100 || cfg.LogLevel != "info" || cfg.SlogLevel() != slog.LevelInfo {
		t.Fatalf("defaults: %+v", cfg)
	}
}

func TestParseEmptyDocumentFailsOnRequiredKeys(t *testing.T) {
	_, err := Parse(strings.NewReader(""))
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("got %v, want error naming listen", err)
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]struct {
		yaml string
		key  string
	}{
		"unknown key":      {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nlisen: x\n", "lisen"},
		"missing broker":   {"listen: 127.0.0.1:0\n", "broker"},
		"broker no port":   {"listen: 127.0.0.1:0\nbroker: localhost\n", "broker"},
		"broker port zero": {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:0\n", "broker"},
		"listen bad":       {"listen: nope\nbroker: 127.0.0.1:1883\n", "listen"},
		"max zero":         {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nmax_clients: 0\n", "max_clients"},
		"max negative":     {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nmax_clients: -1\n", "max_clients"},
		"bad level":        {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nlog_level: trace\n", "log_level"},
		"syntax":           {"listen: [\n", "config"},
	}
	for name, tc := range cases {
		_, err := Parse(strings.NewReader(tc.yaml))
		if err == nil || !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s: got %v, want error mentioning %q", name, err, tc.key)
		}
	}
}

func TestParseListenPortZeroAllowed(t *testing.T) {
	if _, err := Parse(strings.NewReader("listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\n")); err != nil {
		t.Fatal(err)
	}
}
