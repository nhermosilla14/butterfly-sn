package e2e

import (
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestReconnectFromSameAddressReplacesSession: a second CONNECT from the same socket gets CONNACK at once. The old
// broker connection ends with MQTT DISCONNECT, so the old will is not published, and the client gets no DISCONNECT.
func TestReconnectFromSameAddressReplacesSession(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/reconnect/old")
	obs := newObserver(t, b.addr, uniq("e2e-reconnect-old-obs"), topic)
	c := newSNClient(t, gw.addr)
	id := uniq("e2e-reconnect")

	connectWithWillDuration(t, c, id, 60, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte("old-gone"))
	c.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: id})
	expect[mqttsn.Connack](t, c, waitLong)

	obs.expectNone(willNoDeliveryWindow)
	if _, err := c.recv(willNoDeliveryWindow); err == nil {
		t.Fatal("client got a datagram after the reconnect CONNACK, want none")
	}
}

// TestReconnectNewWillAndDurationApply: the replacement session has its own will and duration. Only the new will is
// published, when the replacement goes silent past its new duration.
func TestReconnectNewWillAndDurationApply(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	oldTopic := uniq("e2e/reconnect/apply-old")
	newTopic := uniq("e2e/reconnect/apply-new")
	obsOld := newObserver(t, b.addr, uniq("e2e-reconnect-apply-old-obs"), oldTopic)
	obsNew := newObserver(t, b.addr, uniq("e2e-reconnect-apply-new-obs"), newTopic)
	c := newSNClient(t, gw.addr)
	id := uniq("e2e-reconnect-apply")

	connectWithWillDuration(t, c, id, 60, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: oldTopic}, []byte("old-gone"))
	const payload = "new-gone"
	connectWithWillDuration(t, c, id, 1, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: newTopic}, []byte(payload))

	expect[mqttsn.Disconnect](t, c, waitLong)
	if m := obsNew.next(); m.Topic != newTopic || string(m.Payload) != payload {
		t.Fatalf("will delivered as topic %q payload %q, want %q %q", m.Topic, m.Payload, newTopic, payload)
	}
	obsOld.expectNone(willNoDeliveryWindow)
}

// TestReconnectWithZeroKeepaliveReplaces: with keep-alive 0 on both sessions the replacement still gets CONNACK
// at once, and the old session's will is not published.
func TestReconnectWithZeroKeepaliveReplaces(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/reconnect/zero")
	obs := newObserver(t, b.addr, uniq("e2e-reconnect-zero-obs"), topic)
	c := newSNClient(t, gw.addr)
	id := uniq("e2e-reconnect-zero")

	connectWithWillDuration(t, c, id, 0, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte("zero-gone"))
	connectWithDuration(t, c, id, 0)

	obs.expectNone(willNoDeliveryWindow)
}

// TestReconnectAtMaxClientsOne: with max_clients 1 the only slot is reused by the replacement, so it is accepted.
func TestReconnectAtMaxClientsOne(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayMax(t, b.addr, 1)
	c := newSNClient(t, gw.addr)
	id := uniq("e2e-reconnect-max")

	connect(t, c, id)
	connect(t, c, id)
}

// TestReconnectReplacementAnswersPingreq: the replacement session is the one that answers PINGREQ.
func TestReconnectReplacementAnswersPingreq(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	id := uniq("e2e-reconnect-ping")

	connect(t, c, id)
	connect(t, c, id)
	c.send(mqttsn.Pingreq{})
	expect[mqttsn.Pingresp](t, c, waitShort)
}
