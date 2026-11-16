// Package config loads and validates the gateway's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// Config is the gateway configuration. Unknown keys are rejected.
type Config struct {
	Listen     string `yaml:"listen"`      // UDP host:port for MQTT-SN clients; port 0 picks a free port
	Broker     string `yaml:"broker"`      // MQTT broker TCP host:port
	MaxClients int    `yaml:"max_clients"` // maximum simultaneous sessions
	LogLevel   string `yaml:"log_level"`   // debug, info, warn or error
	GatewayID  int    `yaml:"gateway_id"`  // 1-255, sent in GWINFO and ADVERTISE

	AdvertiseInterval int    `yaml:"advertise_interval"` // seconds between ADVERTISE, 0-65535; 0 disables
	AdvertiseAddress  string `yaml:"advertise_address"`  // UDP host:port for ADVERTISE; empty means broadcast on the listen port

	RetryInterval int `yaml:"retry_interval"` // seconds between broker reconnect attempts, must be greater than 0
	RetryLimit    int `yaml:"retry_limit"`    // broker reconnect attempts before giving up, must be greater than 0
}

var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// Load reads and validates the configuration file at path.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads and validates a YAML configuration. Absent keys take their defaults.
func Parse(r io.Reader) (Config, error) {
	cfg := Config{MaxClients: 100, LogLevel: "info", GatewayID: 1, AdvertiseInterval: 900, RetryInterval: 10, RetryLimit: 5}
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// SlogLevel returns the logging level for LogLevel.
func (c Config) SlogLevel() slog.Level {
	return logLevels[c.LogLevel]
}

func (c Config) validate() error {
	if err := checkAddr("listen", c.Listen, true); err != nil {
		return err
	}
	if err := checkAddr("broker", c.Broker, false); err != nil {
		return err
	}
	if c.MaxClients <= 0 {
		return fmt.Errorf("config: max_clients: must be greater than 0, got %d", c.MaxClients)
	}
	if c.RetryInterval <= 0 {
		return fmt.Errorf("config: retry_interval: must be greater than 0, got %d", c.RetryInterval)
	}
	if c.RetryLimit <= 0 {
		return fmt.Errorf("config: retry_limit: must be greater than 0, got %d", c.RetryLimit)
	}
	if _, ok := logLevels[c.LogLevel]; !ok {
		return fmt.Errorf("config: log_level: must be one of debug, info, warn, error; got %q", c.LogLevel)
	}
	if c.GatewayID < 1 || c.GatewayID > 255 {
		return fmt.Errorf("config: gateway_id: must be between 1 and 255, got %d", c.GatewayID)
	}
	if c.AdvertiseInterval < 0 || c.AdvertiseInterval > 65535 {
		return fmt.Errorf("config: advertise_interval: must be between 0 and 65535, got %d", c.AdvertiseInterval)
	}
	if c.AdvertiseAddress != "" {
		if err := checkAddr("advertise_address", c.AdvertiseAddress, false); err != nil {
			return err
		}
	}
	return nil
}

func checkAddr(key, addr string, allowPortZero bool) error {
	if addr == "" {
		return fmt.Errorf("config: %s: required", key)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("config: %s: must be host:port, got %q", key, addr)
	}
	if !allowPortZero && host == "" {
		return fmt.Errorf("config: %s: host is required, got %q", key, addr)
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || (n == 0 && !allowPortZero) {
		return fmt.Errorf("config: %s: invalid port in %q", key, addr)
	}
	return nil
}
