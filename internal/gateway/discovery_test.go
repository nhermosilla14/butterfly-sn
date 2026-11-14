package gateway

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// startDiscoveryGateway runs a gateway with gateway_id 7 and no broker traffic, with the discovery delay set
// to zero. It returns the gateway and an unconnected client socket.
func startDiscoveryGateway(t *testing.T) (*Gateway, *net.UDPConn) {
	return startDiscoveryGatewayDelay(t, func() time.Duration { return 0 })
}

func startDiscoveryGatewayDelay(t *testing.T, delay func() time.Duration) (*Gateway, *net.UDPConn) {
	t.Helper()
	saved := discoveryDelay
	discoveryDelay = delay
	t.Cleanup(func() { discoveryDelay = saved })

	cfg := config.Config{Listen: "127.0.0.1:0", Broker: "127.0.0.1:1", MaxClients: 10, GatewayID: 7}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		cancel()
		if err := <-runDone; err != nil {
			t.Errorf("gateway Run: %v", err)
		}
	})
	return g, client
}

func gatewaySessions(g *Gateway) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.sessions)
}

func recvSN(t *testing.T, conn *net.UDPConn, timeout time.Duration) (mqttsn.Message, error) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return mqttsn.Decode(buf[:n])
}

func TestSearchgwGetsOneGwinfoWithoutSession(t *testing.T) {
	cases := map[string]mqttsn.Searchgw{
		"no radius":   {},
		"with radius": {Radius: 3, HasRadius: true},
	}
	for name, sg := range cases {
		t.Run(name, func(t *testing.T) {
			g, client := startDiscoveryGateway(t)
			gwAddr := g.Addr().(*net.UDPAddr)
			sendSN(t, client, gwAddr, sg)

			m, err := recvSN(t, client, testWaitLong)
			if err != nil {
				t.Fatalf("waiting for GWINFO: %v", err)
			}
			if got, ok := m.(mqttsn.Gwinfo); !ok || got.GwID != 7 || got.Address != nil {
				t.Fatalf("got %+v, want GWINFO with gateway id 7", m)
			}
			if _, err := recvSN(t, client, testNoDatagram); err == nil {
				t.Fatal("more than one datagram after SEARCHGW")
			}
			if n := gatewaySessions(g); n != 0 {
				t.Fatalf("%d sessions after SEARCHGW, want 0", n)
			}

			sendSN(t, client, gwAddr, mqttsn.Register{MsgID: 1, TopicName: "gw/discovery/after"})
			m, err = recvSN(t, client, testWaitLong)
			if err != nil {
				t.Fatalf("waiting for DISCONNECT: %v", err)
			}
			if _, ok := m.(mqttsn.Disconnect); !ok {
				t.Fatalf("got %+v after SEARCHGW, want DISCONNECT for unknown sender", m)
			}
		})
	}
}

func TestGwinfoAndAdvertiseFromUnknownSenderIgnored(t *testing.T) {
	cases := map[string]mqttsn.Message{
		"gwinfo":    mqttsn.Gwinfo{GwID: 9},
		"advertise": mqttsn.Advertise{GwID: 9, Duration: 900},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			g, client := startDiscoveryGateway(t)
			gwAddr := g.Addr().(*net.UDPAddr)
			sendSN(t, client, gwAddr, m)

			if reply, err := recvSN(t, client, testNoDatagram); err == nil {
				t.Fatalf("got reply %+v, want none", reply)
			}
			if n := gatewaySessions(g); n != 0 {
				t.Fatalf("%d sessions, want 0", n)
			}
		})
	}
}

func TestProductionDiscoveryDelayBounds(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 10000 {
		d := discoveryDelay()
		if d < 0 || d >= maxDiscoveryDelay {
			t.Fatalf("delay %v outside [0, %v)", d, maxDiscoveryDelay)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("delay is constant")
	}
}

func TestSearchgwReplyWithProductionDelayArrivesInTime(t *testing.T) {
	g, client := startDiscoveryGatewayDelay(t, func() time.Duration { return rand.N(maxDiscoveryDelay) })
	sendSN(t, client, g.Addr().(*net.UDPAddr), mqttsn.Searchgw{})
	if m, err := recvSN(t, client, 600*time.Millisecond); err != nil {
		t.Fatalf("waiting for GWINFO: %v", err)
	} else if _, ok := m.(mqttsn.Gwinfo); !ok {
		t.Fatalf("got %+v, want GWINFO", m)
	}
}

func TestMalformedDiscoveryPacketsIgnored(t *testing.T) {
	g, client := startDiscoveryGateway(t)
	gwAddr := g.Addr().(*net.UDPAddr)
	for _, raw := range [][]byte{
		{0x05, 0x01, 0x01, 0x02, 0x03}, // SEARCHGW with too long a body
		{0x02, 0x02},                   // GWINFO without id
		{0x04, 0x00, 0x01, 0x00},       // ADVERTISE too short
	} {
		if _, err := client.WriteToUDP(raw, gwAddr); err != nil {
			t.Fatal(err)
		}
	}
	if m, err := recvSN(t, client, testNoDatagram); err == nil {
		t.Fatalf("got reply %+v to malformed packets", m)
	}
	if n := gatewaySessions(g); n != 0 {
		t.Fatalf("%d sessions, want 0", n)
	}
}

func TestSearchgwThenShutdownRace(t *testing.T) {
	saved := discoveryDelay
	discoveryDelay = func() time.Duration { return time.Millisecond }
	defer func() { discoveryDelay = saved }()
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: "127.0.0.1:1", MaxClients: 10, GatewayID: 7}
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for range 200 {
		g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		gwAddr := g.Addr().(*net.UDPAddr)
		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() { runDone <- g.Run(ctx) }()
		sendSN(t, client, gwAddr, mqttsn.Searchgw{})
		cancel()
		if err := <-runDone; err != nil {
			t.Fatalf("Run: %v", err)
		}
		// Drain what arrived before shutdown; nothing may arrive afterwards.
		for {
			if _, err := recvSN(t, client, time.Millisecond); err != nil {
				break
			}
		}
	}
	if m, err := recvSN(t, client, testNoDatagram); err == nil {
		t.Fatalf("datagram %+v after Run returned", m)
	}
}
