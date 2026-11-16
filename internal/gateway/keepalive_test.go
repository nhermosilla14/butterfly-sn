package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const (
	testPingWait   = 2 * time.Second         // longest acceptable gap for a PINGREQ at CONNECT duration 1 (interval 0.5 s)
	testIdleWindow = 1500 * time.Millisecond // bounded window in which a duration-0 client must cause no PINGREQ
)

// pingBroker is a fake broker. It answers CONNECT, PINGREQ with PINGRESP and QoS 1 PUBLISH with PUBACK,
// and reports each PINGREQ and each published message it receives.
type pingBroker struct {
	ln        net.Listener
	pingreqs  chan time.Time
	published chan publishSeen
	done      chan struct{}
}

func startPingBroker(t *testing.T) *pingBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pb := &pingBroker{
		ln:        ln,
		pingreqs:  make(chan time.Time, 64),
		published: make(chan publishSeen, 64),
		done:      make(chan struct{}),
	}
	go pb.serve()
	return pb
}

func (pb *pingBroker) serve() {
	defer close(pb.done)
	conn, err := pb.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err := mqtt.ReadPacket(conn); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
		return
	}
	for {
		p, err := mqtt.ReadPacket(conn)
		if err != nil {
			return // the gateway closed the connection
		}
		switch {
		case p.Type == 12:
			pb.pingreqs <- time.Now()
			_, _ = conn.Write([]byte{0xD0, 0x00})
		case p.Type == 3 && (p.Flags>>1)&3 == 1:
			n := int(binary.BigEndian.Uint16(p.Body))
			id := binary.BigEndian.Uint16(p.Body[2+n:])
			pb.published <- publishSeen{topic: string(p.Body[2 : 2+n]), id: id, payload: p.Body[4+n:]}
			_, _ = conn.Write([]byte{0x40, 0x02, byte(id >> 8), byte(id)})
		}
	}
}

// startKeepaliveGateway runs a gateway against brokerAddr and returns its UDP address and a client socket.
func startKeepaliveGateway(t *testing.T, brokerAddr string) (*net.UDPAddr, *net.UDPConn) {
	t.Helper()
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: brokerAddr, MaxClients: 10, RetryInterval: 10, RetryLimit: 5}
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
		cancel()
		if err := <-runDone; err != nil {
			t.Errorf("gateway Run: %v", err)
		}
		_ = client.Close()
	})
	return g.Addr().(*net.UDPAddr), client
}

// connectClient performs CONNECT with the given duration and checks CONNACK is accepted.
func connectClient(t *testing.T, client *net.UDPConn, gwAddr *net.UDPAddr, duration uint16) {
	t.Helper()
	sendSN(t, client, gwAddr, mqttsn.Connect{CleanSession: true, Duration: duration, ClientID: "gw-keepalive"})
	ack, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("CONNACK: %v", err)
	}
	if c, ok := ack.(mqttsn.Connack); !ok || c.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK: %+v", ack)
	}
}

func TestKeepaliveSendsPingreqWhenBrokerIdle(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 1)

	// Two PINGREQs in a row, each within the window: the first one half a keepalive after CONNECT.
	for i := 0; i < 2; i++ {
		select {
		case <-pb.pingreqs:
		case <-time.After(testPingWait):
			t.Fatalf("PINGREQ %d not sent within %v of the previous packet", i+1, testPingWait)
		}
	}
	// PINGRESP answers are internal: the client must not get anything for them.
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("datagram for broker PINGRESP: %+v", m)
	}
}

func TestKeepaliveZeroSendsNoPingreq(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 0)

	select {
	case <-pb.pingreqs:
		t.Fatal("PINGREQ sent for keepalive 0")
	case <-time.After(testIdleWindow):
	}
}

// TestKeepaliveDoesNotDisturbQoS1 publishes QoS 1 messages across several keepalive periods. Every
// PUBLISH must reach the broker and get its own PUBACK, in order, while PINGREQ/PINGRESP run alongside.
func TestKeepaliveDoesNotDisturbQoS1(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 1)

	const topic = "gw/keepalive/qos1"
	sendSN(t, client, gwAddr, mqttsn.Register{MsgID: 1, TopicName: topic})
	reg, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("REGACK: %v", err)
	}
	regack, ok := reg.(mqttsn.Regack)
	if !ok {
		t.Fatalf("REGACK: %+v", reg)
	}

	const count = 3
	for i := uint16(1); i <= count; i++ {
		sendSN(t, client, gwAddr, mqttsn.Publish{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: regack.TopicID, MsgID: 0x100 + i, Data: []byte{byte(i)}})
		select {
		case seen := <-pb.published:
			if seen.topic != topic || seen.id != 0x100+i {
				t.Fatalf("broker got topic %q id %#x, want %q id %#x", seen.topic, seen.id, topic, 0x100+i)
			}
		case <-time.After(testWaitLong):
			t.Fatalf("broker never received PUBLISH %d", i)
		}
		m, err := readSN(client, testWaitLong)
		if err != nil {
			t.Fatalf("PUBACK %d: %v", i, err)
		}
		puback, ok := m.(mqttsn.Puback)
		if !ok || puback.MsgID != 0x100+i || puback.TopicID != regack.TopicID {
			t.Fatalf("got %+v, want PUBACK msg id %#x", m, 0x100+i)
		}
		time.Sleep(700 * time.Millisecond) // longer than the 0.5 s keepalive interval
	}

	pings := 0
	for drained := false; !drained; {
		select {
		case <-pb.pingreqs:
			pings++
		default:
			drained = true
		}
	}
	if pings == 0 {
		t.Fatal("no PINGREQ was sent while publishing, so the test proved nothing")
	}
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("extra datagram after PUBACKs: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
}
