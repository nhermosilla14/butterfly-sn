package e2e

import (
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// sleepDuration is the duration a test client asks for when it sleeps: 1 s, so the sleep limit is 1.5 s.
const sleepDuration = 1

// sleepClient sends DISCONNECT with a sleep duration and checks the gateway replies with DISCONNECT.
func sleepClient(t *testing.T, c *snClient, duration uint16) {
	t.Helper()
	c.send(mqttsn.Disconnect{Duration: duration, HasDuration: true})
	expect[mqttsn.Disconnect](t, c, waitShort)
}

// TestSleepExpiryPublishesWillAndFreesSlot: a will client sleeps with a 1 s duration. A second client is refused
// while the sleeper holds the only slot. At expiry the sleeper gets DISCONNECT, the observer receives the will,
// and the second client is accepted.
func TestSleepExpiryPublishesWillAndFreesSlot(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayMax(t, b.addr, 1)
	willTopic := uniq("e2e/sleep/will")
	obs := newObserver(t, b.addr, uniq("e2e-sleep-obs"), willTopic)
	sleeper := newSNClient(t, gw.addr)
	other := newSNClient(t, gw.addr)

	clientID := uniq("e2e-sleep-will")
	connectWithWillDuration(t, sleeper, clientID, 60, mqttsn.WillTopic{Topic: willTopic}, []byte("gone"))
	sleepClient(t, sleeper, sleepDuration)

	other.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: uniq("e2e-sleep-other")})
	if ack := expect[mqttsn.Connack](t, other, waitShort); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("CONNACK return code %d while asleep, want congestion", ack.ReturnCode)
	}

	expect[mqttsn.Disconnect](t, sleeper, waitLong)
	if m := obs.next(); m.Topic != willTopic || string(m.Payload) != "gone" {
		t.Fatalf("will: broker got topic %q payload %q", m.Topic, m.Payload)
	}
	connect(t, other, uniq("e2e-sleep-other-retry"))
}

// TestSleepKeepsSessionAndDisconnectFreesSlot: a client with CONNECT duration 1 sleeps for 5 s. It gets no
// DISCONNECT within the window, so its 1.5 s inactivity limit is not applied. An explicit DISCONNECT then ends
// the session and frees the slot.
func TestSleepKeepsSessionAndDisconnectFreesSlot(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayMax(t, b.addr, 1)
	sleeper := newSNClient(t, gw.addr)
	other := newSNClient(t, gw.addr)

	connectWithDuration(t, sleeper, uniq("e2e-sleep-keep"), 1)
	sleepClient(t, sleeper, 5)
	expectNoDatagram(t, sleeper, waitIdle)

	sleeper.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, sleeper, waitShort)
	connect(t, other, uniq("e2e-sleep-keep-next"))
}

// TestSleepingSubscriberGetsNoDelivery: a subscriber that sleeps gets no delivery while a publisher sends a
// message, then ends the session with DISCONNECT.
func TestSleepingSubscriberGetsNoDelivery(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/sleep/subscriber")
	pub := newPublisher(t, b.addr, uniq("e2e-sleep-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-sleep-sub"))

	if ack := c.subscribe(1, topic, 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK return code %d, want accepted", ack.ReturnCode)
	}
	sleepClient(t, c, 5)

	pub.publish(topic, []byte("while asleep"))
	expectNoDatagram(t, c, waitIdle)

	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitShort)
}
