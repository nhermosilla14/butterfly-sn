// Package e2e drives the gateway only through UDP and checks the broker side through a real Mosquitto.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/app"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const (
	waitShort = 5 * time.Second
	waitLong  = 10 * time.Second
)

// gateway is app.Run in-process on a random UDP port, configured through a temp YAML file.
type gateway struct {
	addr     string
	cancel   context.CancelFunc
	done     chan int
	stdout   *syncBuffer
	stopOnce sync.Once
	code     int
}

func startGateway(t *testing.T, brokerAddr string) *gateway {
	t.Helper()
	return startGatewayMax(t, brokerAddr, 10)
}

// startGatewayMax is startGateway with a given max_clients.
func startGatewayMax(t *testing.T, brokerAddr string, maxClients int) *gateway {
	t.Helper()
	cfgPath := writeConfig(t, fmt.Sprintf("listen: 127.0.0.1:0\nbroker: %s\nmax_clients: %d\nlog_level: debug\n", brokerAddr, maxClients))
	ctx, cancel := context.WithCancel(context.Background())
	g := &gateway{cancel: cancel, done: make(chan int, 1), stdout: &syncBuffer{}}
	go func() { g.done <- app.Run(ctx, []string{"-config", cfgPath}, g.stdout) }()
	t.Cleanup(func() { g.stop(t) })

	g.addr = waitForAddr(t, g.stdout, g.done)
	return g
}

// stop cancels the gateway context and returns the exit status. It is safe to call more than once.
func (g *gateway) stop(t *testing.T) int {
	t.Helper()
	g.stopOnce.Do(func() {
		g.cancel()
		select {
		case g.code = <-g.done:
		case <-time.After(waitLong):
			t.Error("gateway did not stop within 10s")
		}
	})
	return g.code
}

var addrRe = regexp.MustCompile(`gateway started.*addr=(127\.0\.0\.1:\d+)`)

func waitForAddr(t *testing.T, out *syncBuffer, done <-chan int) string {
	t.Helper()
	deadline := time.Now().Add(waitShort)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("gateway exited with %d before starting: %s", code, out.String())
		default:
		}
		if m := addrRe.FindStringSubmatch(out.String()); m != nil {
			return m[1]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("gateway did not log its address: %s", out.String())
	return ""
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "butterfly-sn.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// freePort returns a TCP port on 127.0.0.1 that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// snClient is a raw MQTT-SN client over UDP.
type snClient struct {
	t    *testing.T
	conn *net.UDPConn
}

func newSNClient(t *testing.T, gwAddr string) *snClient {
	t.Helper()
	raddr, err := net.ResolveUDPAddr("udp", gwAddr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &snClient{t: t, conn: conn}
}

func (c *snClient) send(m mqttsn.Message) {
	c.t.Helper()
	b, err := mqttsn.Encode(m)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

// recv waits up to timeout for one datagram and decodes it.
func (c *snClient) recv(timeout time.Duration) (mqttsn.Message, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	buf := make([]byte, 1500)
	n, err := c.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return mqttsn.Decode(buf[:n])
}

// expect receives one message and fails the test unless it is of type T.
func expect[T mqttsn.Message](t *testing.T, c *snClient, timeout time.Duration) T {
	t.Helper()
	m, err := c.recv(timeout)
	if err != nil {
		t.Fatalf("waiting for %T: %v", *new(T), err)
	}
	v, ok := m.(T)
	if !ok {
		t.Fatalf("got %+v, want %T", m, *new(T))
	}
	return v
}

// register sends REGISTER and returns the REGACK. The caller checks the fields it cares about.
func (c *snClient) register(msgID uint16, topic string) mqttsn.Regack {
	c.t.Helper()
	c.send(mqttsn.Register{MsgID: msgID, TopicName: topic})
	return expect[mqttsn.Regack](c.t, c, waitShort)
}

// publishQoS0 sends a QoS 0 PUBLISH on a normal topic id. It expects no reply.
func (c *snClient) publishQoS0(topicID uint16, data []byte) {
	c.t.Helper()
	c.send(mqttsn.Publish{TopicIDType: mqttsn.TopicIDNormal, TopicID: topicID, Data: data})
}

// connect performs CONNECT and asserts CONNACK Accepted.
func connect(t *testing.T, c *snClient, clientID string) {
	t.Helper()
	c.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: clientID})
	ack := expect[mqttsn.Connack](t, c, waitLong)
	if ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK return code %d, want accepted", ack.ReturnCode)
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent write (gateway) and read (test).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(b.buf.String())
}
