package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// brokerPublish is a broker PUBLISH with flags (DUP/QoS/retain bits, header nibble). QoS 1 adds the packet id.
func brokerPublish(flags byte, name string, id uint16, payload []byte) []byte {
	body := topicBody(name)
	if (flags>>1)&3 == 1 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, payload...)
	if len(body) > 127 {
		panic("test PUBLISH too long for a one-byte remaining length")
	}
	return append([]byte{0x30 | flags, byte(len(body))}, body...)
}

// brokerPuback is a broker PUBACK for id, as the broker sends it to the gateway.
func brokerPuback(id uint16) []byte {
	return []byte{0x40, 0x02, byte(id >> 8), byte(id)}
}

// subscribeExact subscribes name through the gateway at msgID with QoS qos and returns the topic id reported.
func (r *subRig) subscribeExact(msgID uint16, name string, qos byte) uint16 {
	r.t.Helper()
	sendSN(r.t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: qos, MsgID: msgID, TopicName: name})
	r.nextBroker()
	r.brokerSend(brokerSuback(msgID, qos))
	ack := readAs[mqttsn.Suback](r.t, r.client)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.TopicID == 0 {
		r.t.Fatalf("SUBACK: %+v", ack)
	}
	return ack.TopicID
}

// fillTopics registers n distinct names through the client, failing unless each is accepted.
func (r *subRig) fillTopics(n int) {
	r.t.Helper()
	for i := 1; i <= n; i++ {
		sendSN(r.t, r.client, r.gwAddr, mqttsn.Register{MsgID: uint16(i), TopicName: "gw/down/fill/" + strconv.Itoa(i)})
		if ack := readAs[mqttsn.Regack](r.t, r.client); ack.ReturnCode != mqttsn.ReturnAccepted {
			r.t.Fatalf("REGISTER %d: %+v", i, ack)
		}
	}
}

// expectBrokerPuback reads the next broker packet, which must be a PUBACK for id.
func (r *subRig) expectBrokerPuback(id uint16) {
	r.t.Helper()
	p := r.nextBroker()
	if p.Type != 4 || string(p.Body) != string([]byte{byte(id >> 8), byte(id)}) {
		r.t.Fatalf("want broker PUBACK for %#x, got type %d body % x", id, p.Type, p.Body)
	}
}

func TestDownlinkKnownTopicForwardedWithoutRegister(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/known"
	id := r.subscribeExact(1, name, 0)

	r.brokerSend(brokerPublish(0x31, name, 0, []byte("hello"))) // QoS 0, retain
	m := readAs[mqttsn.Publish](t, r.client)
	if m.TopicID != id || m.QoS != 0 || !m.Retain || m.DUP || m.MsgID != 0 || string(m.Data) != "hello" {
		t.Fatalf("PUBLISH: %+v, want topic id %#x, QoS 0, retain, payload hello", m, id)
	}
}

func TestDownlinkUnknownTopicRegistersThenReleasesOnAccept(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/unknown"
	r.brokerSend(brokerPublish(0x00, name, 0, []byte("held")))

	reg := readAs[mqttsn.Register](t, r.client)
	if reg.TopicName != name || reg.TopicID == 0 || reg.MsgID == 0 {
		t.Fatalf("REGISTER: %+v", reg)
	}
	expectSilence(t, r.client) // the PUBLISH waits for REGACK

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	m := readAs[mqttsn.Publish](t, r.client)
	if m.TopicID != reg.TopicID || string(m.Data) != "held" {
		t.Fatalf("PUBLISH after REGACK: %+v", m)
	}
}

func TestDownlinkRegackRejectedDropsAndAcksQoS1(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/rejected"
	r.brokerSend(brokerPublish(0x02, name, 0x0101, []byte("dropped")))

	reg := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnInvalidTopicID})
	expectSilence(t, r.client)
	r.expectBrokerPuback(0x0101)
	r.expectNoBrokerPacket()
}

func TestDownlinkQoS1BrokerPubackAfterClientPuback(t *testing.T) {
	for name, code := range map[string]byte{"accepted": mqttsn.ReturnAccepted, "rejected": mqttsn.ReturnCongestion} {
		t.Run(name, func(t *testing.T) {
			r := startSubRig(t)
			const topic = "gw/down/qos1"
			id := r.subscribeExact(1, topic, 1)
			r.brokerSend(brokerPublish(0x02, topic, 0x0202, []byte("q1")))

			m := readAs[mqttsn.Publish](t, r.client)
			if m.QoS != 1 || m.MsgID != 0x0202 || m.TopicID != id || string(m.Data) != "q1" {
				t.Fatalf("PUBLISH: %+v, want QoS 1, msg id 0x0202, topic id %#x", m, id)
			}
			r.expectNoBrokerPacket() // no broker PUBACK before the client answers

			sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: 0x0202, ReturnCode: code})
			r.expectBrokerPuback(0x0202)
			r.expectNoBrokerPacket()
		})
	}
}

func TestDownlinkUnknownRegackAndPubackIgnored(t *testing.T) {
	r := startSubRig(t)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: 1, MsgID: 0x0999, ReturnCode: mqttsn.ReturnAccepted})
	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: 1, MsgID: 0x0999, ReturnCode: mqttsn.ReturnAccepted})
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	// The session still answers the client.
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client)
}

func TestDownlinkSecondPublishJoinsPendingRegistration(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/join"
	r.brokerSend(brokerPublish(0x00, name, 0, []byte("one")))
	r.brokerSend(brokerPublish(0x00, name, 0, []byte("two")))

	reg := readAs[mqttsn.Register](t, r.client)
	expectSilence(t, r.client) // no second REGISTER for the same topic
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	for _, want := range []string{"one", "two"} {
		m := readAs[mqttsn.Publish](t, r.client)
		if m.TopicID != reg.TopicID || string(m.Data) != want {
			t.Fatalf("PUBLISH: %+v, want payload %q on topic id %#x", m, want, reg.TopicID)
		}
	}
	expectSilence(t, r.client)
}

func TestDownlinkHoldFullDropsAndAcksQoS1(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/hold"
	for i := 0; i < maxHeldDownlink; i++ {
		r.brokerSend(brokerPublish(0x00, name, 0, []byte{byte(i)}))
	}
	reg := readAs[mqttsn.Register](t, r.client)

	r.brokerSend(brokerPublish(0x02, name, 0x0501, []byte("overflow")))
	r.expectBrokerPuback(0x0501)
	expectSilence(t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	for i := 0; i < maxHeldDownlink; i++ {
		m := readAs[mqttsn.Publish](t, r.client)
		if len(m.Data) != 1 || m.Data[0] != byte(i) {
			t.Fatalf("held PUBLISH %d: %+v", i, m)
		}
	}
	expectSilence(t, r.client) // the overflow message was not held
}

func TestDownlinkTableFullDropsAndAcksQoS1(t *testing.T) {
	r := startSubRig(t)
	r.fillTopics(maxTopicsPerClient)

	r.brokerSend(brokerPublish(0x02, "gw/down/full/new", 0x0601, []byte("x")))
	r.expectBrokerPuback(0x0601)
	expectSilence(t, r.client) // no REGISTER for a name the table cannot hold

	sendSN(t, r.client, r.gwAddr, mqttsn.Register{MsgID: 0x0700, TopicName: "gw/down/full/extra"})
	if ack := readAs[mqttsn.Regack](t, r.client); ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != 0x0700 {
		t.Fatalf("REGISTER at full table: %+v, want Congestion", ack)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Subscribe{QoS: 0, MsgID: 0x0701, TopicName: "gw/down/full/sub"})
	if ack := readAs[mqttsn.Suback](t, r.client); ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != 0x0701 {
		t.Fatalf("SUBSCRIBE at full table: %+v, want Congestion", ack)
	}
	r.expectNoBrokerPacket()
}

func TestDownlinkDuplicateQoS1IDDroppedWithoutSecondAck(t *testing.T) {
	r := startSubRig(t)
	const topic = "gw/down/dup"
	id := r.subscribeExact(1, topic, 1)
	r.brokerSend(brokerPublish(0x02, topic, 0x0801, []byte("first")))
	m := readAs[mqttsn.Publish](t, r.client)
	if m.TopicID != id || string(m.Data) != "first" {
		t.Fatalf("PUBLISH: %+v", m)
	}

	// The broker reuses id 0x0801 while the client still owes its PUBACK: the second copy is dropped, and
	// the broker is not acknowledged for it, so the first copy keeps its ordering.
	r.brokerSend(brokerPublish(0x02, topic, 0x0801, []byte("second")))
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: 0x0801, ReturnCode: mqttsn.ReturnAccepted})
	r.expectBrokerPuback(0x0801)
	r.expectNoBrokerPacket()
}

func TestDownlinkPendingFullAcksBrokerWithoutSending(t *testing.T) {
	r := startSubRig(t)
	const topic = "gw/down/pending"
	id := r.subscribeExact(1, topic, 1)
	for i := 1; i <= maxPendingDownlink; i++ {
		r.brokerSend(brokerPublish(0x02, topic, uint16(0x1000+i), []byte("p")))
		if m := readAs[mqttsn.Publish](t, r.client); m.MsgID != uint16(0x1000+i) || m.TopicID != id {
			t.Fatalf("PUBLISH %d: %+v", i, m)
		}
	}
	r.brokerSend(brokerPublish(0x02, topic, 0x2000, []byte("extra")))
	r.expectBrokerPuback(0x2000)
	expectSilence(t, r.client)
}

func TestDownlinkBrokerAckWriteFailureIsError(t *testing.T) {
	s := &session{gw: &Gateway{log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	for i := 1; i <= maxTopicsPerClient; i++ {
		if _, ok := s.topics.register("gw/write/fill/" + strconv.Itoa(i)); !ok {
			t.Fatalf("register %d rejected", i)
		}
	}
	broker := closedBrokerClient(t)
	e := mqtt.Event{Kind: mqtt.EventPublish, Topic: "gw/down/write/loss", QoS: 1, PacketID: 4, Payload: []byte("x")}
	if err := s.handleDownlink(broker, e, slog.New(slog.NewTextHandler(io.Discard, nil))); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("PUBACK on closed broker connection: %v, want net.ErrClosed", err)
	}
}

func TestNextRegMsgIDWrapsSkippingZeroAndBusy(t *testing.T) {
	s := &session{pendingRegs: map[uint16]*pendingReg{2: {}}}
	s.regMsgID = 0xFFFE
	if id := s.nextRegMsgID(); id != 0xFFFF {
		t.Fatalf("got %#x, want 0xFFFF", id)
	}
	// Wraps past 0 to 1, and skips 2 while it awaits REGACK.
	if id := s.nextRegMsgID(); id != 1 {
		t.Fatalf("after wrap: got %#x, want 1", id)
	}
	if id := s.nextRegMsgID(); id != 3 {
		t.Fatalf("skipping busy id: got %#x, want 3", id)
	}
}

func TestDownlinkRejectedRegistrationForgetsNameAndRegistersAgain(t *testing.T) {
	r := startSubRig(t)
	const name = "gw/down/reregister"
	r.brokerSend(brokerPublish(0x02, name, 0x0A01, []byte("refused")))
	first := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: first.TopicID, MsgID: first.MsgID, ReturnCode: mqttsn.ReturnCongestion})
	r.expectBrokerPuback(0x0A01)
	expectSilence(t, r.client)

	// The name is gone, so the next PUBLISH registers it again under a new id, and no PUBLISH goes out on the refused one.
	r.brokerSend(brokerPublish(0x00, name, 0, []byte("again")))
	second := readAs[mqttsn.Register](t, r.client)
	if second.TopicName != name || second.TopicID == first.TopicID || second.TopicID == 0 {
		t.Fatalf("second REGISTER %+v, want topic %q with an id other than %#x", second, name, first.TopicID)
	}
	expectSilence(t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: second.TopicID, MsgID: second.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	m := readAs[mqttsn.Publish](t, r.client)
	if m.TopicID != second.TopicID || string(m.Data) != "again" {
		t.Fatalf("PUBLISH after accepted REGACK: %+v", m)
	}
}
