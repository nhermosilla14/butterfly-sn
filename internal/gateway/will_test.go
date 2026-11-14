package gateway

import (
	"bytes"
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

// willBroker is a fake broker. Each accepted connection is reported, its CONNECT is handed to connects, and its
// CONNACK is held until allow is called, so tests can check that CONNACK waits for the broker.
type willBroker struct {
	ln          net.Listener
	accepted    chan struct{}
	connects    chan mqtt.Packet
	release     chan struct{}
	releaseOnce sync.Once
	done        chan struct{}
}

func startWillBroker(t *testing.T) *willBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wb := &willBroker{
		ln:       ln,
		accepted: make(chan struct{}, 4),
		connects: make(chan mqtt.Packet, 4),
		release:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	go wb.serve(t)
	return wb
}

func (wb *willBroker) serve(t *testing.T) {
	defer close(wb.done)
	conn, err := wb.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	wb.accepted <- struct{}{}
	connect, err := mqtt.ReadPacket(conn)
	if err != nil || connect.Type != 1 {
		t.Errorf("will broker: CONNECT: %v", err)
		return
	}
	wb.connects <- connect
	<-wb.release
	if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn) // until the gateway closes the connection
}

// allow releases the held CONNACK. Cleanup calls it too, so a failed test never blocks the broker.
func (wb *willBroker) allow() {
	wb.releaseOnce.Do(func() { close(wb.release) })
}

// nextConnect waits for the broker to receive a CONNECT and returns it.
func (wb *willBroker) nextConnect(t *testing.T) mqtt.Packet {
	t.Helper()
	select {
	case p := <-wb.connects:
		return p
	case <-time.After(testWaitLong):
		t.Fatal("broker received no CONNECT")
	}
	return mqtt.Packet{}
}

// willRig is a gateway running against a willBroker, with one client socket.
type willRig struct {
	t      *testing.T
	g      *Gateway
	gwAddr *net.UDPAddr
	client *net.UDPConn
	broker *willBroker
}

func startWillRig(t *testing.T, maxClients int) *willRig {
	t.Helper()
	wb := startWillBroker(t)
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: wb.ln.Addr().String(), MaxClients: maxClients}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-runDone; err != nil {
			t.Errorf("gateway Run: %v", err)
		}
		wb.allow()
		_ = wb.ln.Close()
		<-wb.done
	})
	r := &willRig{t: t, g: g, gwAddr: g.Addr().(*net.UDPAddr), broker: wb}
	r.client = r.newClient()
	return r
}

// newClient opens a client socket, closed at cleanup.
func (r *willRig) newClient() *net.UDPConn {
	r.t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readAs receives one datagram from the gateway and fails unless it has type T.
func readAs[T mqttsn.Message](t *testing.T, conn *net.UDPConn) T {
	t.Helper()
	m, err := readSN(conn, testWaitLong)
	if err != nil {
		t.Fatalf("waiting for %T: %v", *new(T), err)
	}
	v, ok := m.(T)
	if !ok {
		t.Fatalf("got %+v, want %T", m, *new(T))
	}
	return v
}

// expectSilence fails if the gateway sends anything within the bounded window.
func expectSilence(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	if m, err := readSN(conn, testNoDatagram); err == nil {
		t.Fatalf("unexpected datagram: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
}

// expectNoBrokerContact fails if the broker accepted a connection.
func expectNoBrokerContact(t *testing.T, wb *willBroker) {
	t.Helper()
	select {
	case <-wb.accepted:
		t.Fatal("gateway contacted the broker")
	default:
	}
}

func TestWillHandshakeOrderAndBrokerWill(t *testing.T) {
	r := startWillRig(t, 10)
	const clientID, topic = "will-full", "will/full"
	payload := []byte{0, 0xFF, 'g', 'o', 'n', 'e'}

	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: clientID})
	readAs[mqttsn.WillTopicReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic})
	readAs[mqttsn.WillMsgReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsg{Payload: payload})

	// The broker CONNECT carries the will: flags clean session, will, QoS 1 and retain.
	p := r.broker.nextConnect(t)
	want := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x02 | 0x04 | 0x08 | 0x20, 0, 60, 0, byte(len(clientID))}
	want = append(want, clientID...)
	want = append(want, 0, byte(len(topic)))
	want = append(want, topic...)
	want = append(want, 0, byte(len(payload)))
	want = append(want, payload...)
	if !bytes.Equal(p.Body, want) {
		t.Fatalf("broker CONNECT body % x, want % x", p.Body, want)
	}

	// CONNACK waits for the broker: nothing reaches the client while the broker holds its CONNACK.
	expectSilence(t, r.client)
	r.broker.allow()
	if ack := readAs[mqttsn.Connack](t, r.client); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK %+v, want accepted", ack)
	}
}

func TestWillEmptyTopicSkipsWillMsgReq(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "will-empty"})
	readAs[mqttsn.WillTopicReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopic{})

	// The broker CONNECT has no will. Any WILLMSGREQ would be queued ahead of CONNACK and fail the read below.
	p := r.broker.nextConnect(t)
	if p.Body[7] != 0x02 {
		t.Fatalf("broker CONNECT flags %#x, want clean session only", p.Body[7])
	}
	r.broker.allow()
	if ack := readAs[mqttsn.Connack](t, r.client); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK %+v, want accepted", ack)
	}
}

func TestWillQoS2RefusedWithoutBrokerContact(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "will-qos2"})
	readAs[mqttsn.WillTopicReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopic{QoS: 2, Topic: "will/qos2"})

	if ack := readAs[mqttsn.Connack](t, r.client); ack.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("CONNACK %+v, want not supported", ack)
	}
	expectNoBrokerContact(t, r.broker)
	r.g.mu.Lock()
	defer r.g.mu.Unlock()
	if n := len(r.g.sessions); n != 0 {
		t.Fatalf("%d sessions after QoS 2 refusal, want 0", n)
	}
}

func TestWillDisconnectDuringHandshakeEndsSession(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "will-quit"})
	readAs[mqttsn.WillTopicReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Disconnect{})

	readAs[mqttsn.Disconnect](t, r.client)
	expectNoBrokerContact(t, r.broker)
	r.g.mu.Lock()
	defer r.g.mu.Unlock()
	if n := len(r.g.sessions); n != 0 {
		t.Fatalf("%d sessions after DISCONNECT, want 0", n)
	}
}

// TestWillHandshakeTimeoutFreesSlot: while the handshake runs its slot is held (a second CONNECT gets
// CONNACK congestion); after the timeout the client gets DISCONNECT, no broker contact happens, and the slot is free.
func TestWillHandshakeTimeoutFreesSlot(t *testing.T) {
	r := startWillRig(t, 1)
	other := r.newClient()
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "will-silent"})
	readAs[mqttsn.WillTopicReq](t, r.client)

	sendSN(t, other, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "will-waiting"})
	if ack := readAs[mqttsn.Connack](t, other); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("CONNACK %+v during handshake, want congestion", ack)
	}

	// The handshake timer starts before this read, so the read waits longer than it.
	m, err := readSN(r.client, willHandshakeTimeout+testWaitLong)
	if err != nil {
		t.Fatalf("waiting for DISCONNECT after handshake timeout: %v", err)
	}
	if _, ok := m.(mqttsn.Disconnect); !ok {
		t.Fatalf("got %+v, want DISCONNECT", m)
	}
	expectNoBrokerContact(t, r.broker)

	sendSN(t, other, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "will-waiting"})
	p := r.broker.nextConnect(t)
	if p.Body[7] != 0x02 {
		t.Fatalf("broker CONNECT flags %#x, want clean session only", p.Body[7])
	}
	r.broker.allow()
	if ack := readAs[mqttsn.Connack](t, other); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK %+v after slot freed, want accepted", ack)
	}
}

// TestConnectWithoutWillConnectsImmediately: CONNECT with Will=0 dials the broker at once, with no prompts.
func TestConnectWithoutWillConnectsImmediately(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "no-will"})
	p := r.broker.nextConnect(t)
	if p.Body[7] != 0x02 {
		t.Fatalf("broker CONNECT flags %#x, want clean session only", p.Body[7])
	}
	r.broker.allow()
	if ack := readAs[mqttsn.Connack](t, r.client); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK %+v, want accepted", ack)
	}
}
