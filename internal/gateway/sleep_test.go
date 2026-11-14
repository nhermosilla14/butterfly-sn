package gateway

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// sleepClient sends DISCONNECT with a sleep duration and checks the client gets DISCONNECT back.
func sleepClient(t *testing.T, client *net.UDPConn, gwAddr *net.UDPAddr, duration uint16) {
	t.Helper()
	sendSN(t, client, gwAddr, mqttsn.Disconnect{Duration: duration, HasDuration: true})
	readAs[mqttsn.Disconnect](t, client)
}

// expectNoSessions checks the gateway has no session left. It is safe while the gateway runs.
func expectNoSessions(t *testing.T, g *Gateway) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if n := len(g.sessions); n != 0 {
		t.Fatalf("%d sessions, want 0", n)
	}
}

// TestSleepEntryKeepsBrokerConnectionAndPings: after the sleep DISCONNECT the broker connection stays open and
// the broker keepalive keeps running.
func TestSleepEntryKeepsBrokerConnectionAndPings(t *testing.T) {
	r := startSubRigDuration(t, 1)
	sleepClient(t, r.client, r.gwAddr, 5)

	for i := 0; i < 2; i++ {
		select {
		case p := <-r.broker.packets:
			if p.Type != pktPingreq {
				t.Fatalf("broker packet type %d while asleep, want PINGREQ", p.Type)
			}
		case <-time.After(testPingWait):
			t.Fatalf("no keepalive PINGREQ %d while asleep", i+1)
		}
	}
	select {
	case <-r.broker.closed:
		t.Fatal("broker connection closed while asleep")
	default:
	}
	expectSilence(t, r.client)
}

// TestSleepDoesNotTimeOutAtInactivityLimit: a duration-1 client that sleeps for 5 s gets no DISCONNECT after
// its 1.5 s inactivity limit, and the session is still there afterwards.
func TestSleepDoesNotTimeOutAtInactivityLimit(t *testing.T) {
	r := startSubRigDuration(t, 1)
	sleepClient(t, r.client, r.gwAddr, 5)

	if m, err := readSN(r.client, testSilenceWindow); err == nil {
		t.Fatalf("datagram while asleep: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client) // still asleep: an empty buffer answers PINGREQ at once
	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !s.sleeping {
		t.Fatal("session not asleep")
	}
}

// TestSleepBuffersDownlinkWithoutDelivery: while asleep a QoS 0 PUBLISH reaches no client datagram and gets no
// broker PUBACK; a QoS 1 PUBLISH gets its broker PUBACK at once and still no client datagram.
func TestSleepBuffersDownlinkWithoutDelivery(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/sleep/buffer"
	r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)

	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("qos0")))
	r.brokerSend(brokerPublish(0x02, topic, 0x0A01, []byte("qos1")))
	r.expectBrokerPuback(0x0A01) // the QoS 0 message got no ack, so the first packet is the QoS 1 ack
	r.expectNoBrokerPacket()
	expectSilence(t, r.client)
}

// TestSleepBufferOverflowDropsNewestAndAcksQoS1: the 21st message finds the buffer full. It is dropped, the
// buffered ones are kept in order, and a QoS 1 one is still acknowledged to the broker.
func TestSleepBufferOverflowDropsNewestAndAcksQoS1(t *testing.T) {
	r := startSubRigDuration(t, 60)
	const topic = "gw/sleep/overflow"
	r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 5)

	for i := 0; i < maxSleepBuffer; i++ {
		r.brokerSend(brokerPublish(0x00, topic, 0, []byte{byte(i)}))
	}
	r.brokerSend(brokerPublish(0x02, topic, 0x0B01, []byte("newest")))
	r.expectBrokerPuback(0x0B01)
	expectSilence(t, r.client)

	s := r.session()
	if err := r.stop(); err != nil { // Run returns after the session ends, so its state can be read
		t.Fatalf("gateway Run: %v", err)
	}
	if len(s.sleepBuf) != maxSleepBuffer {
		t.Fatalf("buffer holds %d messages, want %d", len(s.sleepBuf), maxSleepBuffer)
	}
	for i, e := range s.sleepBuf {
		if len(e.Payload) != 1 || e.Payload[0] != byte(i) {
			t.Fatalf("buffer[%d] payload % x, want the %d-th message", i, e.Payload, i)
		}
	}
	if len(s.preAcked) != 0 {
		t.Fatalf("dropped QoS 1 message recorded as pre-acknowledged: %v", s.preAcked)
	}
}

// TestSleepIgnoresOtherPackets: while asleep REGISTER gets no reply, and a broker PUBLISH for an unknown topic is
// buffered without a REGISTER. The session is still asleep afterwards.
func TestSleepIgnoresOtherPackets(t *testing.T) {
	r := startSubRigDuration(t, 60)
	sleepClient(t, r.client, r.gwAddr, 5)

	sendSN(t, r.client, r.gwAddr, mqttsn.Register{MsgID: 1, TopicName: "gw/sleep/reg"})
	expectSilence(t, r.client)
	r.brokerSend(brokerPublish(0x00, "gw/sleep/reg", 0, []byte("held")))
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	s := r.session()
	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !s.sleeping {
		t.Fatal("session left sleep")
	}
}

// TestSleepExpiryWithWillClosesBrokerBare: a will client that sleeps past its limit gets DISCONNECT, the broker
// connection closes without MQTT DISCONNECT so the will is published, and the session is removed.
func TestSleepExpiryWithWillClosesBrokerBare(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("sleep-will", 1, true)
	sleepClient(t, r.client, r.gwAddr, 1)

	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoSessions(t, r.g)
}

// TestSleepExpiryWithoutWillSendsDisconnect: a will-less client that sleeps past its limit gets MQTT DISCONNECT
// to the broker and DISCONNECT to the client.
func TestSleepExpiryWithoutWillSendsDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("sleep-plain", 1, false)
	sleepClient(t, r.client, r.gwAddr, 1)

	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT for a will-less session")
	}
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoSessions(t, r.g)
}

// TestDisconnectWhileAsleepIsNormal: a client DISCONNECT from asleep sends MQTT DISCONNECT to the broker, never
// a bare close, even with a will, and removes the session.
func TestDisconnectWhileAsleepIsNormal(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("sleep-quit", 60, true)
	sleepClient(t, r.client, r.gwAddr, 5)

	sendSN(t, r.client, r.gwAddr, mqttsn.Disconnect{})
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT after client DISCONNECT from asleep")
	}
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoSessions(t, r.g)
}

// TestSecondSleepDisconnectWhileAsleepIsNormal: a DISCONNECT with a duration from asleep does not sleep again. It
// ends the session like any other DISCONNECT.
func TestSecondSleepDisconnectWhileAsleepIsNormal(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("sleep-twice", 60, true)
	sleepClient(t, r.client, r.gwAddr, 5)

	sendSN(t, r.client, r.gwAddr, mqttsn.Disconnect{Duration: 5, HasDuration: true})
	readAs[mqttsn.Disconnect](t, r.client)
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT after second sleep DISCONNECT")
	}
	expectNoSessions(t, r.g)
}

// TestShutdownWhileAsleepSendsDisconnect: gateway shutdown while asleep takes the existing path: MQTT DISCONNECT
// for a will-less session, and the client gets DISCONNECT.
func TestShutdownWhileAsleepSendsDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("sleep-shutdown", 60, false)
	sleepClient(t, r.client, r.gwAddr, 5)

	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT on shutdown while asleep")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}
