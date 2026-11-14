package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const (
	testWaitLong   = 10 * time.Second
	testNoDatagram = 300 * time.Millisecond // bounded window in which no datagram may arrive
)

// publishSeen is what the fake broker received in the MQTT PUBLISH.
type publishSeen struct {
	topic   string
	id      uint16
	payload []byte
}

// fakeBroker accepts one connection, answers CONNECT, reads one QoS 1 PUBLISH and reports it. It holds
// the PUBACK until release is called, then sends an unknown PUBACK id followed by the real one.
type fakeBroker struct {
	ln        net.Listener
	published chan publishSeen
	release   chan struct{}
	releaseMu sync.Once
	done      chan struct{}
}

func startFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{ln: ln, published: make(chan publishSeen, 1), release: make(chan struct{}), done: make(chan struct{})}
	go fb.serve(t)
	return fb
}

func (fb *fakeBroker) serve(t *testing.T) {
	defer close(fb.done)
	conn, err := fb.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	connect, err := mqtt.ReadPacket(conn)
	if err != nil || connect.Type != 1 {
		t.Errorf("fake broker: CONNECT: %v", err)
		return
	}
	if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
		return
	}
	pub, err := mqtt.ReadPacket(conn)
	if err != nil || pub.Type != 3 || (pub.Flags>>1)&3 != 1 {
		t.Errorf("fake broker: want QoS 1 PUBLISH, got type %d flags %#x: %v", pub.Type, pub.Flags, err)
		return
	}
	n := int(pub.Body[0])<<8 | int(pub.Body[1])
	id := uint16(pub.Body[2+n])<<8 | uint16(pub.Body[3+n])
	fb.published <- publishSeen{topic: string(pub.Body[2 : 2+n]), id: id, payload: pub.Body[4+n:]}

	<-fb.release
	// PUBACK for an id the gateway never sent, then the real one.
	_, _ = conn.Write([]byte{0x40, 0x02, 0xEE, 0xEE})
	_, _ = conn.Write([]byte{0x40, 0x02, byte(id >> 8), byte(id)})
	_, _ = io.Copy(io.Discard, conn) // until the gateway closes the connection
}

// freeRelease releases the broker's PUBACK once. Cleanup calls it too, so a failed test never blocks the broker.
func (fb *fakeBroker) freeRelease() {
	fb.releaseMu.Do(func() { close(fb.release) })
}

func readSN(conn *net.UDPConn, timeout time.Duration) (mqttsn.Message, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return mqttsn.Decode(buf[:n])
}

func sendSN(t *testing.T, conn *net.UDPConn, to *net.UDPAddr, m mqttsn.Message) {
	t.Helper()
	b, err := mqttsn.Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.WriteToUDP(b, to); err != nil {
		t.Fatal(err)
	}
}

// TestQoS1PubackWaitsForBrokerPuback drives a real gateway (UDP socket, router, session and broker reader)
// against a fake broker that withholds PUBACK. No MQTT-SN PUBACK may arrive before the broker's PUBACK.
func TestQoS1PubackWaitsForBrokerPuback(t *testing.T) {
	fb := startFakeBroker(t)
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: fb.ln.Addr().String(), MaxClients: 10}
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
	gwAddr := g.Addr().(*net.UDPAddr)
	t.Cleanup(func() {
		cancel()
		if err := <-runDone; err != nil {
			t.Errorf("gateway Run: %v", err)
		}
		fb.freeRelease()
		_ = fb.ln.Close()
		<-fb.done
		_ = client.Close()
	})

	sendSN(t, client, gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "gw-qos1-session"})
	ack, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("CONNACK: %v", err)
	}
	if c, ok := ack.(mqttsn.Connack); !ok || c.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK: %+v", ack)
	}

	const topic = "gw/qos1/session"
	sendSN(t, client, gwAddr, mqttsn.Register{MsgID: 1, TopicName: topic})
	reg, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("REGACK: %v", err)
	}
	regack, ok := reg.(mqttsn.Regack)
	if !ok || regack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK: %+v", reg)
	}

	const msgID = 0x4242
	payload := []byte{0, 0xFF, '\r', '\n', 0x80}
	sendSN(t, client, gwAddr, mqttsn.Publish{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: regack.TopicID, MsgID: msgID, Data: payload})

	var seen publishSeen
	select {
	case seen = <-fb.published:
	case <-time.After(testWaitLong):
		t.Fatal("broker never received the PUBLISH")
	}
	if seen.topic != topic || seen.id != msgID || string(seen.payload) != string(payload) {
		t.Fatalf("broker got topic %q id %#x payload % x", seen.topic, seen.id, seen.payload)
	}

	// The broker has the PUBLISH but has not acknowledged it: the client must get nothing.
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("datagram before broker PUBACK: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}

	fb.freeRelease()
	// The unknown broker PUBACK sends nothing; the real one yields the PUBACK. A datagram for the unknown id
	// would arrive first and fail the check.
	m, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("waiting for PUBACK: %v", err)
	}
	puback, ok := m.(mqttsn.Puback)
	if !ok || puback.MsgID != msgID || puback.TopicID != regack.TopicID || puback.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("got %+v, want PUBACK msg id %#x topic id %#x accepted", m, msgID, regack.TopicID)
	}
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("extra datagram after PUBACK: %+v", m)
	}
}
