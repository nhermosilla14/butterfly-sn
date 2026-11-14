package e2e

import (
	"math/rand/v2"
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// shortAlphabet is the set of characters random short names are drawn from.
const shortAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randomName returns n random characters, so a short topic name is unique per run on the shared broker.
func randomName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = shortAlphabet[rand.IntN(len(shortAlphabet))]
	}
	return string(b)
}

// shortTopicID is the short topic id of a two-character name: its first byte is the high byte.
func shortTopicID(name string) uint16 {
	return uint16(name[0])<<8 | uint16(name[1])
}

func TestShortPublishReachesBrokerLikeNormalPublish(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := randomName(2)
	obs := newObserver(t, b.addr, uniq("e2e-short-pub-obs"), topic)
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-short-pub"))

	short := []byte("via short id")
	c.send(mqttsn.Publish{TopicIDType: mqttsn.TopicIDShort, TopicID: shortTopicID(topic), Data: short})
	if m := obs.next(); m.Topic != topic || string(m.Payload) != string(short) {
		t.Fatalf("broker got topic %q payload %q; want topic %q, payload %q", m.Topic, m.Payload, topic, short)
	}

	reg := c.register(1, topic)
	if reg.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK %+v, want accepted", reg)
	}
	normal := []byte("via normal id")
	c.publishQoS0(reg.TopicID, normal)
	if m := obs.next(); m.Topic != topic || string(m.Payload) != string(normal) {
		t.Fatalf("broker got topic %q payload %q; want topic %q, payload %q", m.Topic, m.Payload, topic, normal)
	}
}

func TestShortSubscribeDeliversShortPublishWithoutRegister(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := randomName(2)
	pub := newPublisher(t, b.addr, uniq("e2e-short-sub-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-short-sub"))

	c.send(mqttsn.Subscribe{QoS: 0, TopicIDType: mqttsn.TopicIDShort, MsgID: 1, TopicName: topic})
	ack := expect[mqttsn.Suback](t, c, waitLong)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.TopicID != shortTopicID(topic) || ack.MsgID != 1 {
		t.Fatalf("SUBACK %+v, want accepted with topic id %#x", ack, shortTopicID(topic))
	}

	payload := []byte("short downlink")
	pub.publish(topic, payload)
	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicIDType != mqttsn.TopicIDShort || m.TopicID != shortTopicID(topic) || string(m.Data) != string(payload) {
		t.Fatalf("PUBLISH %+v, want short topic id %#x and payload %q", m, shortTopicID(topic), payload)
	}
}

func TestShortSubscribeWrongLengthGetsCongestion(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-short-len"))

	c.send(mqttsn.Subscribe{QoS: 0, TopicIDType: mqttsn.TopicIDShort, MsgID: 2, TopicName: randomName(3)})
	if ack := expect[mqttsn.Suback](t, c, waitLong); ack.ReturnCode != mqttsn.ReturnCongestion || ack.MsgID != 2 {
		t.Fatalf("SUBACK %+v, want congestion for a three-character short name", ack)
	}
}

func TestDownlinkOneCharTopicOnWildcardRegistersFirst(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := randomName(1)
	pub := newPublisher(t, b.addr, uniq("e2e-short-one-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-short-one"))

	if ack := c.subscribe(1, "+", 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	payload := []byte("one char")
	pub.publish(topic, payload)

	reg := expect[mqttsn.Register](t, c, waitLong)
	if reg.TopicName != topic || reg.TopicID == 0 {
		t.Fatalf("REGISTER %+v, want topic %q with a nonzero topic id", reg, topic)
	}
	c.send(mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})

	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicIDType != mqttsn.TopicIDNormal || m.TopicID != reg.TopicID || string(m.Data) != string(payload) {
		t.Fatalf("PUBLISH %+v, want normal topic id %#x and payload %q", m, reg.TopicID, payload)
	}
}
