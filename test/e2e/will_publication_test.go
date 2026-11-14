package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// willNoDeliveryWindow bounds how long an observer waits to confirm that no will was published. A published
// will reaches the broker's subscribers within moments of the gateway ending the connection.
const willNoDeliveryWindow = 2 * time.Second

// uniq returns a name unique to this run, for client ids and topics. Wills are retained, so a fixed topic
// would replay the previous run's will to the observer.
func uniq(name string) string {
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
}

// TestWillPublishedOnClientTimeout: a client with a will goes silent past its keepalive. The broker publishes the
// will to an independent subscriber, and the client gets DISCONNECT. A later packet gets DISCONNECT too.
func TestWillPublishedOnClientTimeout(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/will/timeout")
	obs := newObserver(t, b.addr, uniq("e2e-will-timeout-obs"), topic)
	c := newSNClient(t, gw.addr)

	const payload = "timeout-gone"
	connectWithWillDuration(t, c, uniq("e2e-will-timeout"), 1, mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte(payload))
	expect[mqttsn.Disconnect](t, c, waitLong)

	if m := obs.next(); m.Topic != topic || string(m.Payload) != payload {
		t.Fatalf("will delivered as topic %q payload %q, want %q %q", m.Topic, m.Payload, topic, payload)
	}
	c.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/will/timeout/late"})
	expect[mqttsn.Disconnect](t, c, waitShort)
}

// TestWillPublishedOnGatewayShutdown: stopping the gateway closes a will-bearing session without MQTT DISCONNECT,
// so the broker publishes the will. The client gets DISCONNECT and the process exits 0.
func TestWillPublishedOnGatewayShutdown(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/will/shutdown")
	obs := newObserver(t, b.addr, uniq("e2e-will-shutdown-obs"), topic)
	c := newSNClient(t, gw.addr)

	const payload = "shutdown-gone"
	connectWithWill(t, c, uniq("e2e-will-shutdown"), mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte(payload))

	if code := gw.stop(t); code != 0 {
		t.Fatalf("gateway exit status %d, want 0", code)
	}
	expect[mqttsn.Disconnect](t, c, waitShort)
	if m := obs.next(); m.Topic != topic || string(m.Payload) != payload {
		t.Fatalf("will delivered as topic %q payload %q, want %q %q", m.Topic, m.Payload, topic, payload)
	}
}

// TestWillNotPublishedOnExplicitDisconnect: a client DISCONNECT ends the broker connection gracefully, so no
// will is published.
func TestWillNotPublishedOnExplicitDisconnect(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/will/quit")
	obs := newObserver(t, b.addr, uniq("e2e-will-quit-obs"), topic)
	c := newSNClient(t, gw.addr)

	connectWithWill(t, c, uniq("e2e-will-quit"), mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte("quit"))
	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitShort)
	obs.expectNone(willNoDeliveryWindow)
}

// TestWillPublishedOnBrokerLoss: the broker connection is cut. The broker treats it as abnormal and publishes the
// will, and the client gets DISCONNECT.
func TestWillPublishedOnBrokerLoss(t *testing.T) {
	b := startBroker(t)
	proxy := startProxy(t, b.addr)
	gw := startGateway(t, proxy.addr())
	topic := uniq("e2e/will/loss")
	obs := newObserver(t, b.addr, uniq("e2e-will-loss-obs"), topic)
	c := newSNClient(t, gw.addr)

	const payload = "loss-gone"
	connectWithWill(t, c, uniq("e2e-will-loss"), mqttsn.WillTopic{QoS: 1, Retain: true, Topic: topic}, []byte(payload))
	proxy.dropAll()

	if m := obs.next(); m.Topic != topic || string(m.Payload) != payload {
		t.Fatalf("will delivered as topic %q payload %q, want %q %q", m.Topic, m.Payload, topic, payload)
	}
	expect[mqttsn.Disconnect](t, c, waitLong)
}

// TestEmptyWillTopicPublishesNothing: a client that connected with an empty will topic goes silent. It has no
// will, so the broker publishes nothing.
func TestEmptyWillTopicPublishesNothing(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/will/empty")
	obs := newObserver(t, b.addr, uniq("e2e-will-empty-obs"), topic)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Connect{CleanSession: true, Will: true, Duration: 1, ClientID: uniq("e2e-will-empty")})
	expect[mqttsn.WillTopicReq](t, c, waitShort)
	c.send(mqttsn.WillTopic{})
	expect[mqttsn.Connack](t, c, waitLong)

	expect[mqttsn.Disconnect](t, c, waitLong)
	obs.expectNone(willNoDeliveryWindow)
}
