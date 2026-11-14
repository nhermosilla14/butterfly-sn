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

// subBroker is a fake broker. It answers CONNECT, hands the connection to conn and records every later
// packet in packets, in arrival order. Tests send broker packets to the gateway through conn.
type subBroker struct {
	ln      net.Listener
	conn    chan net.Conn
	packets chan mqtt.Packet
	done    chan struct{}
	// closed is closed when the gateway ends the connection.
	closed chan struct{}
}

func startSubBroker(t *testing.T) *subBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sb := &subBroker{ln: ln, conn: make(chan net.Conn, 1), packets: make(chan mqtt.Packet, 64), done: make(chan struct{}), closed: make(chan struct{})}
	go sb.serve()
	return sb
}

// serve answers PINGREQ with PINGRESP, so keepalive tests can run without the test answering pings.
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
			close(sb.closed)
			return
		}
		if p.Type == pktPingreq {
			_, _ = conn.Write([]byte{0xD0, 0x00})
		}
		sb.packets <- p
	}
}

// subRig is a running gateway with one connected client and a fake broker.
type subRig struct {
	t      *testing.T
	gw     *Gateway
	gwAddr *net.UDPAddr
	client *net.UDPConn
	broker *subBroker
	bconn  net.Conn
	// stop shuts the gateway down and returns Run's error. Later calls return the same result.
	stop func() error
}

func startSubRig(t *testing.T) *subRig {
	t.Helper()
	return startSubRigDuration(t, 0)
}

// startSubRigDuration is startSubRig with the client's CONNECT duration.
func startSubRigDuration(t *testing.T, duration uint16) *subRig {
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
		_ = sb.ln.Close()
		<-sb.done
		_ = client.Close()
	})
	r := &subRig{t: t, gw: g, gwAddr: g.Addr().(*net.UDPAddr), client: client, broker: sb, stop: stop}
	sendSN(t, client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: duration, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, client)
	r.bconn = <-sb.conn
	t.Cleanup(func() { _ = r.bconn.Close() })
	return r
}

// session returns the gateway's only session. Its fields belong to the session goroutine, so read them only
// after stop.
func (r *subRig) session() *session {
	r.gw.mu.Lock()
	defer r.gw.mu.Unlock()
	for _, s := range r.gw.sessions {
		return s
	}
	r.t.Fatal("no session")
	return nil
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
	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{TopicIDType: mqttsn.TopicIDPredefined, MsgID: 3, TopicName: "ab"})
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

// shortPublish is a client PUBLISH on a short topic id: the two name bytes as a 16-bit value.
func shortPublish(qos byte, id uint16, msgID uint16, data string) mqttsn.Publish {
	return mqttsn.Publish{QoS: qos, TopicIDType: mqttsn.TopicIDShort, TopicID: id, MsgID: msgID, Data: []byte(data)}
}

func TestShortPublishReachesBrokerOnTwoCharName(t *testing.T) {
	r := startSubRig(t)
	const id = 0x6162 // "ab"
	sendSN(t, r.client, r.gwAddr, shortPublish(0, id, 0, "x"))
	if p := r.nextBroker(); p.Type != 3 || string(p.Body) != string(append(topicBody("ab"), 'x')) {
		t.Fatalf("QoS 0 short PUBLISH: type %d body % x", p.Type, p.Body)
	}

	sendSN(t, r.client, r.gwAddr, shortPublish(1, id, 7, "y"))
	p := r.nextBroker()
	want := append(append(topicBody("ab"), 0, 7), 'y')
	if p.Type != 3 || p.Flags != 0x02 || string(p.Body) != string(want) {
		t.Fatalf("QoS 1 short PUBLISH: type %d flags %#x body % x", p.Type, p.Flags, p.Body)
	}
	r.brokerSend(brokerPuback(7))
	ack := readAs[mqttsn.Puback](t, r.client)
	if ack.TopicID != id || ack.MsgID != 7 || ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("PUBACK: %+v, want topic id %#x, msg id 7", ack, id)
	}
}

func TestShortPublishWithZeroByteDroppedWithoutBrokerContact(t *testing.T) {
	r := startSubRig(t)
	for _, id := range []uint16{0x0061, 0x6100, 0x0000} {
		sendSN(t, r.client, r.gwAddr, shortPublish(0, id, 0, "x"))
		sendSN(t, r.client, r.gwAddr, shortPublish(1, id, 3, "x"))
	}
	r.expectNoBrokerPacket()
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client)
}

func TestShortSubscribeReportsTwoBytesWithoutTableSlot(t *testing.T) {
	r := startSubRig(t)
	r.fillTopics(maxTopicsPerClient)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, TopicIDType: mqttsn.TopicIDShort, MsgID: 0x0900, TopicName: "ab"})
	p := r.nextBroker()
	want := append(append([]byte{0x09, 0x00}, topicBody("ab")...), 1)
	if p.Type != 8 || string(p.Body) != string(want) {
		t.Fatalf("SUBSCRIBE: type %d body % x, want % x", p.Type, p.Body, want)
	}
	r.brokerSend(brokerSuback(0x0900, 1))
	ack := readAs[mqttsn.Suback](t, r.client)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.QoS != 1 || ack.MsgID != 0x0900 || ack.TopicID != 0x6162 {
		t.Fatalf("SUBACK: %+v, want topic id 0x6162", ack)
	}
	// The table is still full, so a normal SUBSCRIBE is refused.
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{MsgID: 0x0901, TopicName: "gw/short/extra"})
	if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("normal SUBSCRIBE at full table: %+v", ack)
	}
}

func TestShortSubscribeWrongLengthCongestionWithoutBrokerContact(t *testing.T) {
	r := startSubRig(t)
	for i, name := range []string{"a", "abc", "a\x00"} {
		id := uint16(0x0A00 + i)
		sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{TopicIDType: mqttsn.TopicIDShort, MsgID: id, TopicName: name})
		if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != id {
			t.Fatalf("short %q: SUBACK %+v, want Congestion", name, ack)
		}
	}
	r.expectNoBrokerPacket()
}

func TestShortSubscribeBrokerRefusalNotTracked(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{TopicIDType: mqttsn.TopicIDShort, MsgID: 0x0B01, TopicName: "ab"})
	r.nextBroker()
	r.brokerSend(brokerSuback(0x0B01, 0x80))
	if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnInvalidTopicID {
		t.Fatalf("SUBACK: %+v", ack)
	}
	r.brokerSend(brokerPublish(0x00, "ab", 0, []byte("x")))
	readAs[mqttsn.Register](t, r.client) // not delivered as a short PUBLISH
}

func TestShortUnsubscribeStopsShortDownlink(t *testing.T) {
	r := startSubRig(t)
	r.subscribeShort(1, "ab")
	r.brokerSend(brokerPublish(0x00, "ab", 0, []byte("one")))
	readAs[mqttsn.Publish](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{TopicIDType: mqttsn.TopicIDShort, MsgID: 9, TopicName: "ab"})
	p := r.nextBroker()
	if p.Type != 10 || string(p.Body) != string(append([]byte{0, 9}, topicBody("ab")...)) {
		t.Fatalf("UNSUBSCRIBE: type %d body % x", p.Type, p.Body)
	}
	r.brokerSend([]byte{0xB0, 0x02, 0, 9})
	if un := readAs[mqttsn.Unsuback](t, r.client); un.MsgID != 9 {
		t.Fatalf("UNSUBACK: %+v", un)
	}

	// The name is no longer short-subscribed, so the next publish takes the normal REGISTER path.
	r.brokerSend(brokerPublish(0x00, "ab", 0, []byte("two")))
	readAs[mqttsn.Register](t, r.client)
}

func TestShortUnsubscribeInvalidLengthAcknowledgedWithoutBrokerContact(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Unsubscribe{TopicIDType: mqttsn.TopicIDShort, MsgID: 4, TopicName: "abc"})
	if un := readAs[mqttsn.Unsuback](t, r.client); un.MsgID != 4 {
		t.Fatalf("UNSUBACK: %+v", un)
	}
	r.expectNoBrokerPacket()
}

// shortNameAt returns a distinct two-character short name for i in 0..255.
func shortNameAt(i int) string {
	return string([]byte{'A' + byte(i/16), 'a' + byte(i%16)})
}

func TestShortSubscribeCapCongestionWithoutBrokerContact(t *testing.T) {
	r := startSubRig(t)
	for i := 0; i < maxShortSubscriptions; i++ {
		r.subscribeShort(uint16(i+1), shortNameAt(i))
	}
	const extra = 0x0F00
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 1, TopicIDType: mqttsn.TopicIDShort, MsgID: extra, TopicName: shortNameAt(maxShortSubscriptions)})
	if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != extra {
		t.Fatalf("SUBACK for name %d: %+v, want Congestion", maxShortSubscriptions+1, ack)
	}
	r.expectNoBrokerPacket()

	// Re-subscribing a tracked name is allowed at the cap and reaches the broker again.
	if id := r.subscribeShort(0x0F01, shortNameAt(0)); id != shortTestID(shortNameAt(0)) {
		t.Fatalf("re-subscribe topic id %#x", id)
	}
}

// shortTestID is the short topic id of a valid two-character test name.
func shortTestID(name string) uint16 {
	id, _ := shortID(name)
	return id
}

func TestTrackedShortNamesCountsTrackedAndPending(t *testing.T) {
	s := &session{
		shortSubs: map[string]struct{}{"ab": {}},
		pendingSubs: map[uint16]pendingSub{
			1: {short: "cd"},
			2: {short: "ab"}, // same name twice counts once
			3: {topicID: 5},  // normal request does not count
		},
	}
	names := s.trackedShortNames()
	if _, ok := names["ab"]; !ok || len(names) != 2 {
		t.Fatalf("tracked names %v, want ab and cd", names)
	}
}

func TestRegisterWildcardRefusedAndNotTabled(t *testing.T) {
	r := startSubRig(t)
	for i, name := range []string{"a/+", "a#", "+"} {
		id := uint16(0x0E00 + i)
		sendSN(t, r.client, r.gwAddr, mqttsn.Register{MsgID: id, TopicName: name})
		ack := readAs[mqttsn.Regack](t, r.client)
		if ack.ReturnCode != mqttsn.ReturnInvalidTopicID || ack.MsgID != id || ack.TopicID != 0 {
			t.Fatalf("REGISTER %q: REGACK %+v, want Invalid topic id", name, ack)
		}
	}
	// Nothing was added, so the first normal name gets the first id.
	sendSN(t, r.client, r.gwAddr, mqttsn.Register{MsgID: 0x0E10, TopicName: "gw/after"})
	if ack := readAs[mqttsn.Regack](t, r.client); ack.TopicID != 1 || ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK after refused names: %+v, want topic id 1", ack)
	}
}

func TestIsPublishableName(t *testing.T) {
	for name, want := range map[string]bool{"": false, "a": true, "a/b": true, "a+": false, "+": false, "a#": false, "#/x": false} {
		if got := isPublishableName(name); got != want {
			t.Errorf("isPublishableName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestShortPublishWithWildcardNameDroppedThenValidWorks(t *testing.T) {
	r := startSubRig(t)
	for _, name := range []string{"a+", "a#"} {
		id := shortTestID(name)
		sendSN(t, r.client, r.gwAddr, shortPublish(0, id, 0, "x"))
		sendSN(t, r.client, r.gwAddr, shortPublish(1, id, 4, "x"))
	}
	r.expectNoBrokerPacket()

	sendSN(t, r.client, r.gwAddr, shortPublish(0, shortTestID("ab"), 0, "ok"))
	if p := r.nextBroker(); p.Type != 3 || string(p.Body) != string(append(topicBody("ab"), "ok"...)) {
		t.Fatalf("valid PUBLISH after drops: type %d body % x", p.Type, p.Body)
	}
}

func TestPublishWildcardTableNameDroppedWithoutBrokerWrite(t *testing.T) {
	// REGISTER refuses wildcard names, so only a table entry seeded directly can hold one.
	s := &session{gw: &Gateway{log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	id, ok := s.topics.register("a+")
	if !ok {
		t.Fatal("seed rejected")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	broker := closedBrokerClient(t) // any write would fail with net.ErrClosed
	for _, m := range []mqttsn.Publish{
		{TopicIDType: mqttsn.TopicIDNormal, TopicID: id, Data: []byte("x")},
		{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: id, MsgID: 6, Data: []byte("x")},
	} {
		if err := s.handlePublish(broker, m, log); err != nil {
			t.Fatalf("PUBLISH %+v: %v, want dropped without broker write", m, err)
		}
	}
}
