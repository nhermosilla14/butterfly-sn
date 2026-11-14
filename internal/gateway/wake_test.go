package gateway

import (
	"net"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// wakePing sends PINGREQ from the client.
func wakePing(r *subRig) {
	r.t.Helper()
	sendSN(r.t, r.client, r.gwAddr, mqttsn.Pingreq{})
}

// expectPublish reads the next datagram, which must be a PUBLISH carrying payload.
func expectPublish(t *testing.T, c *net.UDPConn, payload string) mqttsn.Publish {
	t.Helper()
	m := readAs[mqttsn.Publish](t, c)
	if string(m.Data) != payload {
		t.Fatalf("PUBLISH payload %q, want %q", m.Data, payload)
	}
	return m
}

// TestWakeEmptyPingreqGetsPingrespAndStaysAsleep: with an empty buffer PINGREQ gets PINGRESP at once. The client
// stays asleep and the broker connection stays open.
func TestWakeEmptyPingreqGetsPingrespAndStaysAsleep(t *testing.T) {
	r := startSubRigDuration(t, 60)
	sleepClient(t, r.client, r.gwAddr, 5)
	for i := 0; i < 2; i++ {
		wakePing(r)
		readAs[mqttsn.Pingresp](t, r.client)
	}
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
	select {
	case <-r.broker.closed:
		t.Fatal("broker connection closed by an empty PINGREQ wake")
	default:
	}

	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !s.sleeping || s.flushing {
		t.Fatalf("sleeping %v flushing %v, want asleep and not flushing", s.sleeping, s.flushing)
	}
}

// TestWakeFlushesQoS0InOrderThenPingresp: buffered QoS 0 messages arrive in order, PINGRESP is the last datagram,
// and the client sleeps again so the next message is buffered.
func TestWakeFlushesQoS0InOrderThenPingresp(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/qos0"
	id := r.subscribeExact(1, topic, 0)
	sleepClient(t, r.client, r.gwAddr, 5)
	for _, p := range []string{"one", "two", "three"} {
		r.brokerSend(brokerPublish(0x00, topic, 0, []byte(p)))
	}
	expectSilence(t, r.client)

	wakePing(r)
	for _, p := range []string{"one", "two", "three"} {
		if m := expectPublish(t, r.client, p); m.TopicID != id || m.QoS != 0 {
			t.Fatalf("PUBLISH %+v, want topic id %#x QoS 0", m, id)
		}
	}
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)

	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("four")))
	expectSilence(t, r.client)
	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !s.sleeping || s.flushing || len(s.sleepBuf) != 1 {
		t.Fatalf("sleeping %v flushing %v buffered %d, want asleep, not flushing, 1 buffered", s.sleeping, s.flushing, len(s.sleepBuf))
	}
}

// TestWakeUnknownTopicRegistersBeforePingresp: a buffered topic the client does not know is REGISTERed at wake.
// Nothing else is sent until REGACK; then the PUBLISH and finally PINGRESP.
func TestWakeUnknownTopicRegistersBeforePingresp(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/unknown"
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("held")))
	expectSilence(t, r.client)

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	if reg.TopicName != topic || reg.TopicID == 0 || reg.MsgID == 0 {
		t.Fatalf("REGISTER %+v", reg)
	}
	expectSilence(t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	if m := expectPublish(t, r.client, "held"); m.TopicID != reg.TopicID {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, reg.TopicID)
	}
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
}

// TestWakeRejectedRegackDropsBufferedAndFinishes: a rejected REGACK drops the buffered message. It was already
// acknowledged to the broker when buffered, so no second broker PUBACK follows. PINGRESP still ends the wake.
func TestWakeRejectedRegackDropsBufferedAndFinishes(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/rejected"
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x02, topic, 0x0301, []byte("dropped")))
	r.expectBrokerPuback(0x0301)

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnInvalidTopicID})
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}

// TestWakeQoS1GetsGatewayMsgIDAndNoSecondBrokerPuback: a buffered QoS 1 message is sent with a nonzero gateway
// msg id. The client's PUBACK for it produces no second broker PUBACK.
func TestWakeQoS1GetsGatewayMsgIDAndNoSecondBrokerPuback(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/qos1"
	id := r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x02, topic, 0x0401, []byte("q1")))
	r.expectBrokerPuback(0x0401)

	wakePing(r)
	m := expectPublish(t, r.client, "q1")
	if m.QoS != 1 || m.MsgID == 0 || m.TopicID != id {
		t.Fatalf("PUBLISH %+v, want QoS 1, nonzero msg id, topic id %#x", m, id)
	}
	readAs[mqttsn.Pingresp](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: m.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	r.expectNoBrokerPacket()
	expectSilence(t, r.client)

	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if len(s.preAcked) != 0 {
		t.Fatalf("pre-acknowledged ids left after client PUBACK: %v", s.preAcked)
	}
}

// TestWakeDuplicateBrokerIDGetsDistinctMsgIDs: the broker reused packet id 0x0501 for two buffered messages. Each
// gets its own gateway msg id, and each client PUBACK is matched without a second broker PUBACK.
func TestWakeDuplicateBrokerIDGetsDistinctMsgIDs(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/dup"
	id := r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x02, topic, 0x0501, []byte("first")))
	r.expectBrokerPuback(0x0501)
	r.brokerSend(brokerPublish(0x02, topic, 0x0501, []byte("second")))
	r.expectBrokerPuback(0x0501)

	wakePing(r)
	a := expectPublish(t, r.client, "first")
	b := expectPublish(t, r.client, "second")
	if a.MsgID == 0 || b.MsgID == 0 || a.MsgID == b.MsgID {
		t.Fatalf("msg ids %#x and %#x, want two distinct nonzero ids", a.MsgID, b.MsgID)
	}
	readAs[mqttsn.Pingresp](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: a.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: b.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	r.expectNoBrokerPacket()
}

// TestWakeConnectFlushesAndStaysActive: CONNECT from asleep gets CONNACK at once, with no broker contact. The
// buffer is flushed without PINGRESP, and later broker PUBLISHes are delivered normally.
func TestWakeConnectFlushesAndStaysActive(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/connect"
	id := r.subscribeExact(1, topic, 0)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("buffered")))
	expectSilence(t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, r.client)
	if m := expectPublish(t, r.client, "buffered"); m.TopicID != id {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, id)
	}
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("live")))
	expectPublish(t, r.client, "live")
	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if s.sleeping || s.flushing {
		t.Fatalf("sleeping %v flushing %v, want active", s.sleeping, s.flushing)
	}
}

// TestWakeConnectRearmsInactivityWithOriginalDuration: the CONNECT from asleep asks for duration 60, which is
// ignored. The active inactivity limit comes from the original CONNECT duration of 1 s, so a silent client is
// dropped.
func TestWakeConnectRearmsInactivityWithOriginalDuration(t *testing.T) {
	r := startSubRigDuration(t, 1)
	sleepClient(t, r.client, r.gwAddr, 5)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, r.client)

	readAs[mqttsn.Disconnect](t, r.client)
}

// TestWakeDuringFlushDeliveredAfterBufferedBeforePingresp: while a flush waits for REGACK, a second PINGREQ is
// ignored and a new broker PUBLISH joins the end of the queue. Both are delivered in order, then PINGRESP.
func TestWakeDuringFlushDeliveredAfterBufferedBeforePingresp(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const known, fresh = "gw/wake/known", "gw/wake/fresh"
	id := r.subscribeExact(1, known, 0)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x00, fresh, 0, []byte("first")))
	expectSilence(t, r.client)

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	wakePing(r)
	r.brokerSend(brokerPublish(0x00, known, 0, []byte("second")))
	expectSilence(t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	if m := expectPublish(t, r.client, "first"); m.TopicID != reg.TopicID {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, reg.TopicID)
	}
	if m := expectPublish(t, r.client, "second"); m.TopicID != id {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, id)
	}
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
}

// TestWakeRestartsSleepTimerAfterPingreq: a PINGREQ wake restarts the sleep timer. A client that sleeps for 2 s
// (limit 3 s) wakes at 2 s and must still be alive at 4 s.
func TestWakeRestartsSleepTimerAfterPingreq(t *testing.T) {
	r := startSubRigDuration(t, 60)
	sleepClient(t, r.client, r.gwAddr, 2)
	time.Sleep(2 * time.Second)
	wakePing(r)
	readAs[mqttsn.Pingresp](t, r.client)

	time.Sleep(2 * time.Second)
	wakePing(r)
	readAs[mqttsn.Pingresp](t, r.client)
}

// TestSleepMovesHeldRegisterToBufferAndWakeRegistersAgain: a PUBLISH held for a gateway REGISTER that the client
// never answered is moved into the sleep buffer on sleep, and its QoS 1 is acknowledged to the broker then. The
// wake sends a fresh REGISTER. A late REGACK for the old one is unknown and ignored. After the new REGACK the
// message is delivered and PINGRESP is last.
func TestSleepMovesHeldRegisterToBufferAndWakeRegistersAgain(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/held"
	r.brokerSend(brokerPublish(0x02, topic, 0x0D01, []byte("pending")))
	old := readAs[mqttsn.Register](t, r.client)
	expectSilence(t, r.client)

	sleepClient(t, r.client, r.gwAddr, 5)
	r.expectBrokerPuback(0x0D01)
	expectSilence(t, r.client)

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	if reg.TopicName != topic || reg.TopicID == 0 || reg.MsgID == old.MsgID {
		t.Fatalf("fresh REGISTER %+v, want topic %q with a new msg id (old %#x)", reg, topic, old.MsgID)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: old.TopicID, MsgID: old.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	expectSilence(t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	m := expectPublish(t, r.client, "pending")
	if m.TopicID != reg.TopicID || m.QoS != 1 || m.MsgID == 0 {
		t.Fatalf("PUBLISH %+v, want topic id %#x at QoS 1 with a msg id", m, reg.TopicID)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}

// TestWakePreAckedCapDropsFurtherQoS1: with 20 pre-acknowledged msg ids awaiting client PUBACKs, the next buffered
// QoS 1 PUBLISH is dropped on wake: it is not sent, it is not acknowledged to the broker again, and the wake still
// ends with PINGRESP.
func TestWakePreAckedCapDropsFurtherQoS1(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/cap"
	id := r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)
	for i := 0; i < maxPreAcked; i++ {
		packetID := uint16(0x0E00 + i)
		r.brokerSend(brokerPublish(0x02, topic, packetID, []byte{byte(i)}))
		r.expectBrokerPuback(packetID)
	}
	wakePing(r)
	for i := 0; i < maxPreAcked; i++ {
		if m := readAs[mqttsn.Publish](t, r.client); m.QoS != 1 || m.TopicID != id {
			t.Fatalf("PUBLISH %d %+v, want QoS 1 on topic id %#x", i, m, id)
		}
	}
	readAs[mqttsn.Pingresp](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, r.client)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x02, topic, 0x0F01, []byte("over cap")))
	r.expectBrokerPuback(0x0F01)

	wakePing(r)
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if len(s.preAcked) != maxPreAcked {
		t.Fatalf("pre-acknowledged set holds %d ids, want %d", len(s.preAcked), maxPreAcked)
	}
}

// TestActiveQoS1WithPreAckedIDIsDropped: an active downlink QoS 1 PUBLISH whose broker packet id equals a gateway
// msg id still awaiting the client's PUBACK is dropped and acknowledged to the broker, with no duplicate msg id sent.
func TestActiveQoS1WithPreAckedIDIsDropped(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/wake/collide"
	id := r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x02, topic, 0x0F10, []byte("buffered")))
	r.expectBrokerPuback(0x0F10)

	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "gw-subscriptions"})
	readAs[mqttsn.Connack](t, r.client)
	m := expectPublish(t, r.client, "buffered")
	if m.TopicID != id || m.MsgID == 0 {
		t.Fatalf("PUBLISH %+v, want topic id %#x and a msg id", m, id)
	}
	expectSilence(t, r.client)

	r.brokerSend(brokerPublish(0x02, topic, m.MsgID, []byte("collides")))
	r.expectBrokerPuback(m.MsgID)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}
