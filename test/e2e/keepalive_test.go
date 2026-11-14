package e2e

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// waitIdle is a bounded period longer than the duration-1 inactivity limit (1.5 s).
const waitIdle = 2500 * time.Millisecond

// TestIdleClientSurvivesBrokerKeepalive connects with a 1 s keepalive and stays idle for longer than
// the broker's 1.5 s limit, then publishes QoS 1. The client sends PINGREQ every 500 ms, so it is not silent
// for the gateway's inactivity limit. Without gateway PINGREQs the broker would have dropped the session.
func TestIdleClientSurvivesBrokerKeepalive(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	const topic = "e2e/keepalive/idle"
	obs := newObserver(t, b.addr, "e2e-keepalive-obs", topic)
	c := newSNClient(t, gw.addr)
	c.send(mqttsn.Connect{CleanSession: true, Duration: 1, ClientID: "e2e-keepalive-idle"})
	if ack := expect[mqttsn.Connack](t, c, waitLong); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK return code %d, want accepted", ack.ReturnCode)
	}

	reg := c.register(1, topic)
	if reg.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK return code %d, want accepted", reg.ReturnCode)
	}
	for i := 0; i < 6; i++ {
		time.Sleep(500 * time.Millisecond)
		c.send(mqttsn.Pingreq{})
		expect[mqttsn.Pingresp](t, c, waitShort)
	}

	const msgID = 0x0101
	payload := []byte("still here")
	c.send(mqttsn.Publish{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: reg.TopicID, MsgID: msgID, Data: payload})

	m := obs.next()
	if m.Topic != topic || m.Retain || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("broker got topic %q retain %v payload %q; want topic %q, payload %q", m.Topic, m.Retain, m.Payload, topic, payload)
	}
	ack := expect[mqttsn.Puback](t, c, waitLong)
	if ack.MsgID != msgID || ack.TopicID != reg.TopicID || ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("PUBACK %+v, want msg id %#x, topic id %#x, accepted", ack, msgID, reg.TopicID)
	}
}

// TestClientPingGetsPingresp checks that PINGREQ is answered by the gateway and the session stays usable:
// a QoS 0 and a QoS 1 publish after the ping still reach the broker.
func TestClientPingGetsPingresp(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	const topic = "e2e/liveness/ping"
	obs := newObserver(t, b.addr, "e2e-ping-obs", topic)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-ping")
	reg := c.register(1, topic)

	c.send(mqttsn.Pingreq{})
	expect[mqttsn.Pingresp](t, c, waitShort)

	payload := []byte("after ping qos0")
	c.publishQoS0(reg.TopicID, payload)
	if m := obs.next(); m.Topic != topic || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("QoS 0 after ping: broker got topic %q payload %q", m.Topic, m.Payload)
	}

	const msgID = 0x0202
	payload = []byte("after ping qos1")
	c.send(mqttsn.Publish{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: reg.TopicID, MsgID: msgID, Data: payload})
	if m := obs.next(); m.Topic != topic || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("QoS 1 after ping: broker got topic %q payload %q", m.Topic, m.Payload)
	}
	ack := expect[mqttsn.Puback](t, c, waitLong)
	if ack.MsgID != msgID || ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("PUBACK %+v, want msg id %#x accepted", ack, msgID)
	}
}

// TestSilentClientExpiresAndFreesSlot uses max_clients 1 and a 1 s keepalive. A client that stays silent gets a
// best-effort DISCONNECT after its 1.5 s limit, its session is gone, and a second client's CONNECT is accepted.
func TestSilentClientExpiresAndFreesSlot(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayMax(t, b.addr, 1)
	silent := newSNClient(t, gw.addr)
	connectWithDuration(t, silent, "e2e-expire-a", 1)

	expect[mqttsn.Disconnect](t, silent, waitLong)

	// The session is gone: a REGISTER from the expired client gets DISCONNECT, not REGACK.
	silent.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/liveness/expire"})
	expect[mqttsn.Disconnect](t, silent, waitShort)

	// The slot is free. Retry within a bounded window, as the session removal is asynchronous to the client.
	next := newSNClient(t, gw.addr)
	deadline := time.Now().Add(waitLong)
	for {
		next.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "e2e-expire-b"})
		m, err := next.recv(waitShort)
		if err != nil {
			t.Fatalf("waiting for CONNACK: %v", err)
		}
		ack, ok := m.(mqttsn.Connack)
		if !ok {
			t.Fatalf("got %+v, want CONNACK", m)
		}
		if ack.ReturnCode == mqttsn.ReturnAccepted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot still taken after client expiry: CONNACK return code %d", ack.ReturnCode)
		}
	}
}

// TestZeroKeepaliveNeverExpires connects with duration 0. The client stays connected through an idle period
// longer than the duration-1 limit, and PINGREQ still gets PINGRESP and publishes still work.
func TestZeroKeepaliveNeverExpires(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	const topic = "e2e/liveness/zero"
	obs := newObserver(t, b.addr, "e2e-zero-obs", topic)
	c := newSNClient(t, gw.addr)
	connectWithDuration(t, c, "e2e-zero", 0)
	reg := c.register(1, topic)

	if m, err := c.recv(waitIdle); err == nil {
		t.Fatalf("datagram for silent duration-0 client: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}

	c.send(mqttsn.Pingreq{})
	expect[mqttsn.Pingresp](t, c, waitShort)

	payload := []byte("zero keepalive")
	c.publishQoS0(reg.TopicID, payload)
	if m := obs.next(); m.Topic != topic || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("broker got topic %q payload %q", m.Topic, m.Payload)
	}
}
