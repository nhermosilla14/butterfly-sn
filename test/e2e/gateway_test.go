package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/app"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

func TestStartupInvalidConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.yaml")
	cases := map[string]struct {
		args []string
		key  string
	}{
		"missing file":     {[]string{"-config", missing}, "config"},
		"no config flag":   {nil, "-config"},
		"invalid yaml":     {[]string{"-config", writeConfig(t, "listen: [\n")}, "config"},
		"unknown key":      {[]string{"-config", writeConfig(t, "listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nlisen: x\n")}, "lisen"},
		"missing listen":   {[]string{"-config", writeConfig(t, "broker: 127.0.0.1:1883\n")}, "listen"},
		"zero max_clients": {[]string{"-config", writeConfig(t, "listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nmax_clients: 0\n")}, "max_clients"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			code := app.Run(context.Background(), tc.args, &out)
			if code == 0 {
				t.Fatalf("exit status 0, want non-zero; output: %s", out.String())
			}
			if !strings.Contains(out.String(), tc.key) {
				t.Fatalf("output does not mention %q: %s", tc.key, out.String())
			}
		})
	}
}

func TestConnectAccepted(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	connect(t, c, "e2e-accepted")

	b.waitForLog(t, `New client connected from \S+ as e2e-accepted \(`, 1)
}

func TestConnectBrokerUnreachable(t *testing.T) {
	closed := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	gw := startGateway(t, closed)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "e2e-unreachable"})
	ack := expect[mqttsn.Connack](t, c, waitLong)
	if ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("CONNACK return code %d, want congestion (1)", ack.ReturnCode)
	}
}

func TestDisconnect(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-disconnect")

	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitLong)

	b.waitForLog(t, `Client e2e-disconnect \[\S+\] disconnected`, 1)
}

func TestSignalShutdown(t *testing.T) {
	bin := buildBinary(t)
	for name, sig := range map[string]os.Signal{"SIGINT": syscall.SIGINT, "SIGTERM": syscall.SIGTERM} {
		t.Run(name, func(t *testing.T) {
			b := startBroker(t)
			cfg := writeConfig(t, fmt.Sprintf("listen: 127.0.0.1:0\nbroker: %s\n", b.addr))
			var out syncBuffer
			cmd := exec.Command(bin, "-config", cfg)
			cmd.Stdout = &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })

			addr := waitForProcessAddr(t, &out, exited)
			c := newSNClient(t, addr)
			connect(t, c, "e2e-signal-"+name)

			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("exit after %s: %v; output: %s", name, err, out.String())
				}
			case <-time.After(waitLong):
				t.Fatalf("process did not exit after %s", name)
			}
			b.waitForLog(t, `Client e2e-signal-`+name+` \[\S+\] disconnected`, 1)
		})
	}
}

// buildBinary compiles cmd/butterfly-sn so the signal test runs the real process.
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "butterfly-sn")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/nhermosilla14/butterfly-sn/cmd/butterfly-sn").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

var procAddrRe = regexp.MustCompile(`gateway started.*addr=(127\.0\.0\.1:\d+)`)

func waitForProcessAddr(t *testing.T, out *syncBuffer, exited <-chan error) string {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for time.Now().Before(deadline) {
		select {
		case err := <-exited:
			t.Fatalf("process exited before starting: %v; output: %s", err, out.String())
		default:
		}
		if m := procAddrRe.FindStringSubmatch(out.String()); m != nil {
			return m[1]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process did not log its address: %s", out.String())
	return ""
}

func TestShutdownClosesBrokerAndLeavesNoGoroutines(t *testing.T) {
	b := startBroker(t)
	before := runtime.NumGoroutine()
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-cancel")

	if code := gw.stop(t); code != 0 {
		t.Fatalf("exit status %d after cancel, want 0; output: %s", code, gw.stdout.String())
	}
	b.waitForLog(t, `Client e2e-cancel \[\S+\] disconnected`, 1)

	deadline := time.Now().Add(waitShort)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines after shutdown: %d, before start: %d", n, before)
	}
}
