package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	brokerEnv      = "BUTTERFLY_E2E_BROKER"
	mosquittoImage = "eclipse-mosquitto:2"
	mosquittoPort  = "1883/tcp"
	startTimeout   = 2 * time.Minute
	// The config is passed through the environment and written by the entrypoint, so no file copy
	// races the broker start. Listen on all interfaces: the image default is local-only.
	mosquittoScript = `printf '%s\n' "$MOSQUITTO_CONF" > /tmp/mosquitto.conf && exec /usr/sbin/mosquitto -c /tmp/mosquitto.conf`
	mosquittoConf   = `listener 1883 0.0.0.0
allow_anonymous true
persistence false
log_dest stdout
log_type error
log_type warning
log_type notice
log_type information
connection_messages true
`
)

// broker is a real Mosquitto. It is either the shared container started by startBroker, or an
// external broker from BUTTERFLY_E2E_BROKER, whose logs are then not visible (ctr is nil).
type broker struct {
	addr string
	ctr  testcontainers.Container
}

var (
	sharedMu     sync.Mutex
	sharedBroker *broker
	sharedErr    error
)

// startBroker returns the broker shared by this test package, starting the container on first use.
// Tests keep to unique client ids, so they can share the broker and its log.
func startBroker(t *testing.T) *broker {
	t.Helper()
	if addr := os.Getenv(brokerEnv); addr != "" {
		return &broker{addr: addr}
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedBroker == nil && sharedErr == nil {
		sharedBroker, sharedErr = runMosquitto(context.Background())
	}
	if sharedErr != nil {
		t.Fatalf("e2e broker unavailable: %v\n"+
			"Docker must be reachable by this user (for example: sg docker -c 'go test ./test/e2e/'). "+
			"To use a running broker instead, set %s=host:port.", sharedErr, brokerEnv)
	}
	return sharedBroker
}

// TestMain terminates the shared broker container after all tests of the package have run.
func TestMain(m *testing.M) {
	code := m.Run()
	sharedMu.Lock()
	if sharedBroker != nil && sharedBroker.ctr != nil {
		if err := testcontainers.TerminateContainer(sharedBroker.ctr); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: terminate broker container:", err)
		}
	}
	sharedMu.Unlock()
	os.Exit(code)
}

// runMosquitto starts eclipse-mosquitto:2 and returns its mapped address.
func runMosquitto(ctx context.Context) (*broker, error) {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mosquittoImage,
			ExposedPorts: []string{mosquittoPort},
			Env:          map[string]string{"MOSQUITTO_CONF": mosquittoConf},
			Entrypoint:   []string{"sh", "-c", mosquittoScript},
			WaitingFor:   wait.ForLog("running").WithStartupTimeout(startTimeout),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", mosquittoImage, err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return nil, terminateOnError(ctr, err)
	}
	port, err := ctr.MappedPort(ctx, mosquittoPort)
	if err != nil {
		return nil, terminateOnError(ctr, err)
	}
	return &broker{addr: fmt.Sprintf("%s:%s", host, port.Port()), ctr: ctr}, nil
}

func terminateOnError(ctr testcontainers.Container, err error) error {
	_ = testcontainers.TerminateContainer(ctr)
	return err
}

// logText returns the broker's log, or skips the calling test when the broker is external.
func (b *broker) logText(t *testing.T) string {
	t.Helper()
	if b.ctr == nil {
		t.Skip("BUTTERFLY_E2E_BROKER is set: broker log is not available")
	}
	rc, err := b.ctr.Logs(context.Background())
	if err != nil {
		t.Fatalf("broker logs: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("broker logs: %v", err)
	}
	return string(data)
}

// waitForLog polls the broker log until pattern matches exactly want times or the wait expires.
func (b *broker) waitForLog(t *testing.T, pattern string, want int) {
	t.Helper()
	re := regexp.MustCompile(pattern)
	deadline := time.Now().Add(waitShort)
	var got int
	for {
		got = len(re.FindAllStringIndex(b.logText(t), -1))
		if got >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got != want {
		t.Fatalf("broker log: pattern %q matched %d times, want %d; log:\n%s",
			pattern, got, want, strings.TrimSpace(b.logText(t)))
	}
}
