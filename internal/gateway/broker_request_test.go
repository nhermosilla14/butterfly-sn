package gateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// MQTT control packet types the broker tests check.
const (
	pktSubscribe   = 8
	pktUnsubscribe = 10
)

// silentBroker is a fake broker that accepts any number of connections. Each one gets CONNACK, is handed to conns
// so the test can reply on it, and has its later packets recorded in packets. It never answers SUBSCRIBE or
// UNSUBSCRIBE, and PINGREQ gets PINGRESP.
type silentBroker struct {
	ln      net.Listener
	conns   chan net.Conn
	packets chan mqtt.Packet
	wg      sync.WaitGroup
	mu      sync.Mutex
	open    []net.Conn
}

func startSilentBroker(t *testing.T) *silentBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sb := &silentBroker{ln: ln, conns: make(chan net.Conn, 4), packets: make(chan mqtt.Packet, 64)}
	sb.wg.Add(1)
	go func() {
		defer sb.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			sb.mu.Lock()
			sb.open = append(sb.open, conn)
			sb.mu.Unlock()
			sb.wg.Add(1)
			go sb.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		sb.mu.Lock()
		for _, c := range sb.open {
			_ = c.Close()
		}
		sb.mu.Unlock()
		sb.wg.Wait()
	})
	return sb
}

func (sb *silentBroker) serve(conn net.Conn) {
	defer sb.wg.Done()
	if _, err := mqtt.ReadPacket(conn); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
		return
	}
	sb.conns <- conn
	for {
		p, err := mqtt.ReadPacket(conn)
		if err != nil {
			return
		}
		if p.Type == pktPingreq {
			_, _ = conn.Write([]byte{0xD0, 0x00})
		}
		sb.packets <- p
	}
}

// silentRig is a gateway with one client and a silentBroker. bconn is the broker side of the current connection.
type silentRig struct {
	t      *testing.T
	gw     *Gateway
	gwAddr *net.UDPAddr
	client *net.UDPConn
	broker *silentBroker
	bconn  net.Conn
	// stop shuts the gateway down and returns Run's error. Later calls return the same result.
	stop func() error
}

// startSilentRig starts a gateway with the given retry interval and limit, and connects clientID.
func startSilentRig(t *testing.T, clientID string, interval time.Duration, limit int) *silentRig {
	t.Helper()
	sb := startSilentBroker(t)
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: sb.ln.Addr().String(), MaxClients: 10, RetryInterval: 10, RetryLimit: 5}
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	g.retryInterval, g.retryLimit = interval, limit
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	var stopOnce sync.Once
	var runErr error
	stop := func() error {
		stopOnce.Do(func() {
			cancel()
			runErr = <-runDone
		})
		return runErr
	}
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("gateway Run: %v", err)
		}
		_ = client.Close()
	})
	r := &silentRig{t: t, gw: g, gwAddr: g.Addr().(*net.UDPAddr), client: client, broker: sb, stop: stop}
	r.connect(clientID)
	return r
}

// connect sends CONNECT for clientID from the client, waits for CONNACK and takes the broker side of the new
// connection.
func (r *silentRig) connect(clientID string) {
	r.t.Helper()
	sendSN(r.t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: clientID})
	readAs[mqttsn.Connack](r.t, r.client)
	select {
	case r.bconn = <-r.broker.conns:
	case <-time.After(testWaitLong):
		r.t.Fatal("broker received no CONNECT")
	}
}

// expectBrokerPacketType reads the next broker packet, which must have type typ.
func (r *silentRig) expectBrokerPacketType(typ byte) {
	r.t.Helper()
	select {
	case p := <-r.broker.packets:
		if p.Type != typ {
			r.t.Fatalf("broker packet type %d, want %d", p.Type, typ)
		}
	case <-time.After(testWaitLong):
		r.t.Fatalf("no broker packet type %d", typ)
	}
}

// expectDisconnectSkippingRegister waits for the client's DISCONNECT. REGISTER retransmissions that come first are
// skipped; anything else fails the test.
func expectDisconnectSkippingRegister(t *testing.T, client *net.UDPConn) {
	t.Helper()
	for {
		m, err := readSN(client, testWaitLong)
		if err != nil {
			t.Fatalf("waiting for DISCONNECT: %v", err)
		}
		switch m.(type) {
		case mqttsn.Register:
			continue
		case mqttsn.Disconnect:
			return
		default:
			t.Fatalf("got %+v, want DISCONNECT", m)
		}
	}
}

// TestBrokerRequestSubscribeUnansweredDisconnectsAndFreesSlot: a SUBSCRIBE the broker never answers is sent once,
// never retransmitted, and after the budget the client gets DISCONNECT. The session is gone and the broker
// connection is closed without MQTT DISCONNECT.
func TestBrokerRequestSubscribeUnansweredDisconnectsAndFreesSlot(t *testing.T) {
	r := startSilentRig(t, "req-sub", 50*time.Millisecond, 2)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 3, TopicName: "gw/req/sub"})
	r.expectBrokerPacketType(pktSubscribe)

	expectDisconnectSkippingRegister(t, r.client)
	expectNoSessions(t, r.gw)
	select {
	case <-r.broker.packets:
		t.Fatal("broker packet after SUBSCRIBE: want none, a request is not retransmitted")
	case <-time.After(testNoDatagram):
	}
}

// TestBrokerRequestUnsubscribeUnansweredDisconnectsAndFreesSlot: the same for an UNSUBSCRIBE.
func TestBrokerRequestUnsubscribeUnansweredDisconnectsAndFreesSlot(t *testing.T) {
	r := startSilentRig(t, "req-unsub", 50*time.Millisecond, 2)
	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{MsgID: 4, TopicName: "gw/req/unsub"})
	r.expectBrokerPacketType(pktUnsubscribe)

	expectDisconnectSkippingRegister(t, r.client)
	expectNoSessions(t, r.gw)
}

// TestBrokerRequestAnsweredInsideBudgetKeepsSession: a SUBSCRIBE answered inside the budget clears its deadline.
// No later DISCONNECT comes, and the client still gets PINGRESP after the budget has passed.
func TestBrokerRequestAnsweredInsideBudgetKeepsSession(t *testing.T) {
	r := startSilentRig(t, "req-answered", 50*time.Millisecond, 2)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 5, TopicName: "gw/req/answered"})
	r.expectBrokerPacketType(pktSubscribe)
	if _, err := r.bconn.Write(brokerSuback(5, 1)); err != nil {
		t.Fatal(err)
	}
	if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnAccepted || ack.MsgID != 5 {
		t.Fatalf("SUBACK: %+v", ack)
	}

	expectSilence(t, r.client) // three times the budget
	expectPingresp(t, r.client, r.gwAddr, "req-answered")
}

// TestBrokerRequestPendingWhileSleepingStillTimesOut: a SUBSCRIBE still pending when the client sleeps keeps its
// broker deadline. The broker is expected to answer whatever the client does, so the session ends after the
// budget even though the client sleeps for much longer.
func TestBrokerRequestPendingWhileSleepingStillTimesOut(t *testing.T) {
	r := startSilentRig(t, "req-sleep", 50*time.Millisecond, 2)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 6, TopicName: "gw/req/sleep"})
	r.expectBrokerPacketType(pktSubscribe)
	sleepClient(t, r.client, r.gwAddr, 60)

	expectDisconnectSkippingRegister(t, r.client)
	expectNoSessions(t, r.gw)
}

// TestTeardownReconnectStartsClean: after a broker request loss with a pending REGISTER, a pending SUBSCRIBE and a
// downlink PUBLISH, the same client id connects again to a new session. An old REGACK and an old SUBACK are
// ignored, and the first downlink for the topic needs a new REGISTER.
func TestTeardownReconnectStartsClean(t *testing.T) {
	const clientID = "teardown-id"
	r := startSilentRig(t, clientID, 200*time.Millisecond, 1)
	const topic = "gw/teardown/down"
	if _, err := r.bconn.Write(brokerPublish(0x00, topic, 0, []byte("held"))); err != nil {
		t.Fatal(err)
	}
	reg := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 9, TopicName: "gw/teardown/sub"})
	r.expectBrokerPacketType(pktSubscribe)

	expectDisconnectSkippingRegister(t, r.client)
	expectNoSessions(t, r.gw)

	r.connect(clientID)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	if _, err := r.bconn.Write(brokerSuback(9, 1)); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, r.client)

	if _, err := r.bconn.Write(brokerPublish(0x00, topic, 0, []byte("fresh"))); err != nil {
		t.Fatal(err)
	}
	if again := readAs[mqttsn.Register](t, r.client); again.TopicName != topic {
		t.Fatalf("REGISTER %+v, want a new one for %q", again, topic)
	}
}
