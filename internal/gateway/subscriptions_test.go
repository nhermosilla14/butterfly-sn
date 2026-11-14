package gateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/config"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// subBroker is a fake broker. It answers CONNECT, hands the connection to conn and records every later
// packet in packets, in arrival order. Tests send broker packets to the gateway through conn.
type subBroker struct {
	ln      net.Listener
	conn    chan net.Conn
	packets chan mqtt.Packet
	done    chan struct{}
}

func startSubBroker(t *testing.T) *subBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sb := &subBroker{ln: ln, conn: make(chan net.Conn, 1), packets: make(chan mqtt.Packet, 64), done: make(chan struct{})}
	go sb.serve()
	return sb
}

func (sb *subBroker) serve() {
	defer close(sb.done)
	conn, err := sb.ln.Accept()
	if err != nil {
		return
	}
	if _, err := mqtt.ReadPacket(conn); err != nil {
		_ = conn.Close()
		return
	}
	if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
		_ = conn.Close()
		return
	}
	sb.conn <- conn
	for {
		p, err := mqtt.ReadPacket(conn)
		if err != nil {
			_ = conn.Close()
			return
		}
		sb.packets <- p
	}
}

// subRig is a running gateway with one connected client and a fake broker.
type subRig struct {
	t      *testing.T
	gwAddr *net.UDPAddr
	client *net.UDPConn
	broker *subBroker
	bconn  net.Conn
}

func startSubRig(t *testing.T) *subRig {
	t.Helper()
	sb := startSubBroker(t)
	cfg := config.Config{Listen: "127.0.0.1:0", Broker: sb.ln.Addr().String(), MaxClients: 10}
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
		_ = sb.ln.Close()
		<-sb.done
		_ = client.Close()
	})
	r := &subRig{t: t, gwAddr: g.Addr().(*net.UDPAddr), client: client, broker: sb}
	sendSN(t, client, r.gwAddr, mqttsn.Connect{CleanSession: true, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, client)
	r.bconn = <-sb.conn
	t.Cleanup(func() { _ = r.bconn.Close() })
	return r
}

// nextBroker returns the next packet the broker received, failing if none arrives in time.
func (r *subRig) nextBroker() mqtt.Packet {
	r.t.Helper()
	select {
	case p := <-r.broker.packets:
		return p
	case <-r.t.Context().Done():
		r.t.Fatal("test ended")
	case <-time.After(testWaitLong):
		r.t.Fatal("broker received no packet")
	}
	return mqtt.Packet{}
}

// expectNoBrokerPacket fails if the broker receives anything within the bounded window.
func (r *subRig) expectNoBrokerPacket() {
	r.t.Helper()
	select {
	case p := <-r.broker.packets:
		r.t.Fatalf("unexpected broker packet type %d", p.Type)
	case <-time.After(testNoDatagram):
	}
}

// brokerSend writes raw bytes from the fake broker to the gateway.
func (r *subRig) brokerSend(b []byte) {
	r.t.Helper()
	if _, err := r.bconn.Write(b); err != nil {
		r.t.Fatal(err)
	}
}

// topicBody is the MQTT string encoding of name, as a body prefix.
func topicBody(name string) []byte {
	return append([]byte{0, byte(len(name))}, name...)
}

// brokerSuback is a broker SUBACK for id; code is the granted QoS (0 or 1) or 0x80 for a refusal.
func brokerSuback(id uint16, code byte) []byte {
	return []byte{0x90, 0x03, byte(id >> 8), byte(id), code}
}

func TestSubscribeExactForwardsRegistersAndReportsTopicID(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/sub/exact"
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 5, TopicName: name})

	p := r.nextBroker()
	want := append(append([]byte{0, 5}, topicBody(name)...), 1)
	if p.Type != 8 || p.Flags != 0x02 || string(p.Body) != string(want) {
		t.Fatalf("SUBSCRIBE: type %d flags %#x body % x, want body % x", p.Type, p.Flags, p.Body, want)
	}
	r.brokerSend(brokerSuback(5, 1))
	ack := readAs[mqttsn.Suback](t, r.client)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.QoS != 1 || ack.MsgID != 5 || ack.TopicID == 0 {
		t.Fatalf("SUBACK: %+v", ack)
	}

	// The table now maps ack.TopicID to name: a PUBLISH on that id reaches the broker on the name.
	sendSN(t, r.client, r.gwAddr, mqttsn.Publish{TopicID: ack.TopicID, Data: []byte("x")})
	pub := r.nextBroker()
	if pub.Type != 3 || string(pub.Body) != string(append(topicBody(name), 'x')) {
		t.Fatalf("PUBLISH after SUBACK: type %d body % x", pub.Type, pub.Body)
	}
}

func TestSubscribeWildcardReportsZeroAndRegistersNothing(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 0, MsgID: 6, TopicName: "gw/sub/+/x"})
	r.nextBroker()
	r.brokerSend(brokerSuback(6, 0))
	ack := readAs[mqttsn.Suback](t, r.client)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.QoS != 0 || ack.TopicID != 0 || ack.MsgID != 6 {
		t.Fatalf("SUBACK: %+v", ack)
	}

	// Nothing was registered, so the first topic id is unknown and its PUBLISH is dropped.
	sendSN(t, r.client, r.gwAddr, mqttsn.Publish{TopicID: 1, Data: []byte("x")})
	r.expectNoBrokerPacket()
}

func TestSubscribeBrokerRefusalMapsToInvalidTopicIDWithQoS0(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, MsgID: 7, TopicName: "gw/sub/refused"})
	r.nextBroker()
	r.brokerSend(brokerSuback(7, 0x80))
	ack := readAs[mqttsn.Suback](t, r.client)
	// ADR downlink-delivery-and-topics: a broker refusal is return code 0x02 with QoS 0 and topic id 0.
	if ack.ReturnCode != mqttsn.ReturnInvalidTopicID || ack.QoS != 0 || ack.TopicID != 0 || ack.MsgID != 7 {
		t.Fatalf("SUBACK: %+v", ack)
	}
}

func TestSubscribeUnsupportedRefusedWithoutBrokerContact(t *testing.T) {
	cases := map[string]mqttsn.Subscribe{
		"qos 2":      {QoS: 2, TopicIDType: mqttsn.TopicIDNormal, MsgID: 1, TopicName: "a"},
		"predefined": {TopicIDType: mqttsn.TopicIDPredefined, MsgID: 2, TopicName: "ab"},
		"short":      {TopicIDType: mqttsn.TopicIDShort, MsgID: 3, TopicName: "ab"},
	}
	for name, m := range cases {
		r := startSubRig(t)
		sendSN(t, r.client, r.gwAddr, m)
		ack := readAs[mqttsn.Suback](t, r.client)
		if ack.ReturnCode != mqttsn.ReturnNotSupported || ack.MsgID != m.MsgID || ack.TopicID != 0 {
			t.Errorf("%s: SUBACK %+v", name, ack)
		}
		r.expectNoBrokerPacket()
	}
}

func TestSubscribePendingOverflowIsCongestion(t *testing.T) {
	r := startSubRig(t)
	for i := uint16(1); i <= maxPendingSubscriptions; i++ {
		sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{MsgID: i, TopicName: "gw/sub/overflow"})
		r.nextBroker()
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{MsgID: 0x0100, TopicName: "gw/sub/extra"})
	ack := readAs[mqttsn.Suback](t, r.client)
	if ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != 0x0100 {
		t.Fatalf("SUBACK: %+v", ack)
	}
	r.expectNoBrokerPacket()
}

func TestUnsubscribeForwardsAndKeepsTopicEntry(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/unsub/exact"
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{MsgID: 4, TopicName: name})
	r.nextBroker()
	r.brokerSend(brokerSuback(4, 0))
	ack := readAs[mqttsn.Suback](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{MsgID: 9, TopicName: name})
	p := r.nextBroker()
	want := append([]byte{0, 9}, topicBody(name)...)
	if p.Type != 10 || p.Flags != 0x02 || string(p.Body) != string(want) {
		t.Fatalf("UNSUBSCRIBE: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
	r.brokerSend([]byte{0xB0, 0x02, 0, 9})
	un := readAs[mqttsn.Unsuback](t, r.client)
	if un.MsgID != 9 {
		t.Fatalf("UNSUBACK: %+v", un)
	}

	sendSN(t, r.client, r.gwAddr, mqttsn.Publish{TopicID: ack.TopicID, Data: []byte("y")})
	if pub := r.nextBroker(); pub.Type != 3 {
		t.Fatalf("PUBLISH after UNSUBACK: type %d", pub.Type)
	}
}

func TestUnsubscribeUnsupportedAcknowledgedWithoutBrokerContact(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{TopicIDType: mqttsn.TopicIDShort, MsgID: 3, TopicName: "ab"})
	if un := readAs[mqttsn.Unsuback](t, r.client); un.MsgID != 3 {
		t.Fatalf("UNSUBACK: %+v", un)
	}
	r.expectNoBrokerPacket()
}

func TestUnsubscribePendingOverflowIsDropped(t *testing.T) {
	r := startSubRig(t)
	for i := uint16(1); i <= maxPendingUnsubscriptions; i++ {
		sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{MsgID: i, TopicName: "gw/unsub/overflow"})
		r.nextBroker()
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{MsgID: 0x0100, TopicName: "gw/unsub/extra"})
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}

func TestUnknownSubackAndUnsubackAreIgnored(t *testing.T) {
	r := startSubRig(t)
	r.brokerSend(brokerSuback(0x0777, 0))
	r.brokerSend([]byte{0xB0, 0x02, 0x07, 0x78})
	expectSilence(t, r.client)

	// The session still answers the client.
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client)
}
