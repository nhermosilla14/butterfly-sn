package config

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse(strings.NewReader("listen: 0.0.0.0:5684\nbroker: 127.0.0.1:1883\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxClients != 100 || cfg.LogLevel != "info" || cfg.SlogLevel() != slog.LevelInfo || cfg.GatewayID != 1 ||
		cfg.AdvertiseInterval != 900 || cfg.AdvertiseAddress != "" || cfg.RetryInterval != 10 || cfg.RetryLimit != 5 {
		t.Fatalf("defaults: %+v", cfg)
	}
}

func TestParseEmptyDocumentFailsOnRequiredKeys(t *testing.T) {
	_, err := Parse(strings.NewReader(""))
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("got %v, want error naming listen", err)
	}
}

const base = "listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\n"

func TestParseErrors(t *testing.T) {
	cases := map[string]struct {
		yaml string
		key  string
	}{
		"unknown key":              {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nlisen: x\n", "lisen"},
		"missing broker":           {"listen: 127.0.0.1:0\n", "broker"},
		"broker no port":           {"listen: 127.0.0.1:0\nbroker: localhost\n", "broker"},
		"broker port zero":         {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:0\n", "broker"},
		"listen bad":               {"listen: nope\nbroker: 127.0.0.1:1883\n", "listen"},
		"max zero":                 {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nmax_clients: 0\n", "max_clients"},
		"max negative":             {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nmax_clients: -1\n", "max_clients"},
		"bad level":                {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nlog_level: trace\n", "log_level"},
		"syntax":                   {"listen: [\n", "config"},
		"gateway id zero":          {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\ngateway_id: 0\n", "gateway_id"},
		"gateway id neg":           {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\ngateway_id: -1\n", "gateway_id"},
		"gateway id 256":           {"listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\ngateway_id: 256\n", "gateway_id"},
		"advertise interval neg":   {base + "advertise_interval: -1\n", "advertise_interval"},
		"advertise interval 65536": {base + "advertise_interval: 65536\n", "advertise_interval"},
		"advertise addr no port":   {base + "advertise_address: 10.0.0.1\n", "advertise_address"},
		"advertise addr port zero": {base + "advertise_address: 10.0.0.1:0\n", "advertise_address"},
		"advertise addr no host":   {base + "advertise_address: \":5684\"\n", "advertise_address"},
		"advertise addr garbage":   {base + "advertise_address: \"!!\"\n", "advertise_address"},
		"retry interval zero":      {base + "retry_interval: 0\n", "retry_interval"},
		"retry interval negative":  {base + "retry_interval: -1\n", "retry_interval"},
		"retry limit zero":         {base + "retry_limit: 0\n", "retry_limit"},
		"retry limit negative":     {base + "retry_limit: -1\n", "retry_limit"},
	}
	for name, tc := range cases {
		_, err := Parse(strings.NewReader(tc.yaml))
		if err == nil || !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s: got %v, want error mentioning %q", name, err, tc.key)
		}
	}
}

func TestParseGatewayIDBounds(t *testing.T) {
	for _, id := range []string{"1", "255"} {
		cfg, err := Parse(strings.NewReader("listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\ngateway_id: " + id + "\n"))
		if err != nil {
			t.Fatalf("gateway_id %s: %v", id, err)
		}
		if strconv.Itoa(cfg.GatewayID) != id {
			t.Fatalf("gateway_id %s parsed as %d", id, cfg.GatewayID)
		}
	}
}

func TestParseListenPortZeroAllowed(t *testing.T) {
	if _, err := Parse(strings.NewReader("listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\n")); err != nil {
		t.Fatal(err)
	}
}

func TestParseAdvertise(t *testing.T) {
	for _, iv := range []int{0, 65535} {
		cfg, err := Parse(strings.NewReader(base + "advertise_interval: " + strconv.Itoa(iv) + "\nadvertise_address: 10.0.0.1:1234\n"))
		if err != nil {
			t.Fatalf("interval %d: %v", iv, err)
		}
		if cfg.AdvertiseInterval != iv || cfg.AdvertiseAddress != "10.0.0.1:1234" {
			t.Fatalf("interval %d parsed as %+v", iv, cfg)
		}
	}
}

func TestParseRetryExplicit(t *testing.T) {
	cfg, err := Parse(strings.NewReader(base + "retry_interval: 3\nretry_limit: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RetryInterval != 3 || cfg.RetryLimit != 7 {
		t.Fatalf("retry parsed as %+v", cfg)
	}
}
