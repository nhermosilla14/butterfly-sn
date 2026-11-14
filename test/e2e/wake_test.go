package e2e

import (
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestWakePingreqFlushesInOrderThenPingresp: messages published while the client sleeps are delivered in order on
// PINGREQ, the QoS 1 one is acknowledged by the client, PINGRESP comes last, and the client sleeps again.
func TestWakePingreqFlushesInOrderThenPingresp(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/wake/flush")
	pub := newPublisher(t, b.addr, uniq("e2e-wake-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-wake"))
	ack := c.subscribe(1, topic, 1)
	if ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	sleepClient(t, c, 5)

	pub.publish(topic, []byte("one"))
	pub.publish(topic, []byte("two"))
	pub.publishQoS1(topic, []byte("three"), 11)
	expectNoDatagram(t, c, downlinkWindow) // the messages are buffered before the PINGREQ is sent

	c.send(mqttsn.Pingreq{})
	for _, want := range []struct {
		payload string
		qos     byte
	}{{"one", 0}, {"two", 0}, {"three", 1}} {
		m := expect[mqttsn.Publish](t, c, waitLong)
		if m.TopicID != ack.TopicID || m.QoS != want.qos || string(m.Data) != want.payload {
			t.Fatalf("PUBLISH %+v, want payload %q at QoS %d on topic id %#x", m, want.payload, want.qos, ack.TopicID)
		}
		if m.QoS == 1 {
			if m.MsgID == 0 {
				t.Fatal("QoS 1 PUBLISH with msg id 0")
			}
			c.send(mqttsn.Puback{TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.ReturnAccepted})
		}
	}
	expect[mqttsn.Pingresp](t, c, waitLong)
	expectNoDatagram(t, c, downlinkWindow)

	pub.publish(topic, []byte("asleep again"))
	expectNoDatagram(t, c, downlinkWindow)
	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitShort)
}

// TestWakeWildcardUnregisteredTopicRegistersBeforePingresp: a topic the client never registered is published while
// it sleeps. On PINGREQ the client gets REGISTER, answers it, then gets the PUBLISH and PINGRESP last.
func TestWakeWildcardUnregisteredTopicRegistersBeforePingresp(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	base := uniq("e2e/wake/wild")
	pub := newPublisher(t, b.addr, uniq("e2e-wake-wild-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-wake-wild"))
	if ack := c.subscribe(1, base+"/#", 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	sleepClient(t, c, 5)

	topic := base + "/new"
	payload := []byte("announced on wake")
	pub.publish(topic, payload)
	expectNoDatagram(t, c, downlinkWindow)

	c.send(mqttsn.Pingreq{})
	reg := expect[mqttsn.Register](t, c, waitLong)
	if reg.TopicName != topic || reg.TopicID == 0 {
		t.Fatalf("REGISTER %+v, want topic %q with a nonzero topic id", reg, topic)
	}
	expectNoDatagram(t, c, downlinkWindow) // nothing else until REGACK
	c.send(mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})

	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicID != reg.TopicID || string(m.Data) != string(payload) {
		t.Fatalf("PUBLISH %+v, want topic id %#x and payload %q", m, reg.TopicID, payload)
	}
	expect[mqttsn.Pingresp](t, c, waitLong)
	expectNoDatagram(t, c, downlinkWindow)
}

// TestWakeConnectFlushesThenDeliversLive: CONNECT from asleep gets CONNACK, then the buffered payload with no
// PINGRESP. The session stays active, so a later live publish arrives normally.
func TestWakeConnectFlushesThenDeliversLive(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/wake/connect")
	pub := newPublisher(t, b.addr, uniq("e2e-wake-connect-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-wake-connect"))
	ack := c.subscribe(1, topic, 0)
	if ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	sleepClient(t, c, 5)
	pub.publish(topic, []byte("buffered"))
	expectNoDatagram(t, c, downlinkWindow)

	c.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: uniq("e2e-wake-connect-again")})
	if connack := expect[mqttsn.Connack](t, c, waitShort); connack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK return code %d, want accepted", connack.ReturnCode)
	}
	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicID != ack.TopicID || string(m.Data) != "buffered" {
		t.Fatalf("PUBLISH %+v, want buffered payload on topic id %#x", m, ack.TopicID)
	}
	expectNoDatagram(t, c, downlinkWindow)

	pub.publish(topic, []byte("live"))
	if live := expect[mqttsn.Publish](t, c, waitLong); string(live.Data) != "live" {
		t.Fatalf("live PUBLISH %+v, want payload live", live)
	}
	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitShort)
}
