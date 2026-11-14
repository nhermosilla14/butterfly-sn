package e2e

import (
	"bytes"
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

func TestRegisterReturnsStableTopicID(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-reg-stable")
	const topic = "e2e/register/stable"

	first := c.register(1, topic)
	if first.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK return code %d, want accepted", first.ReturnCode)
	}
	if first.TopicID == 0 || first.TopicID == 0xFFFF {
		t.Fatalf("REGACK topic id %#x, want 1 to 0xFFFE", first.TopicID)
	}
	if first.MsgID != 1 {
		t.Fatalf("REGACK msg id %d, want 1", first.MsgID)
	}

	again := c.register(2, topic)
	if again.ReturnCode != mqttsn.ReturnAccepted || again.TopicID != first.TopicID || again.MsgID != 2 {
		t.Fatalf("second REGISTER of %q: %+v, want topic id %#x, msg id 2, accepted", topic, again, first.TopicID)
	}
}

func TestPublishQoS0ReachesBroker(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	const topic = "e2e/publish/qos0"
	obs := newObserver(t, b.addr, "e2e-pub-obs", topic)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-pub-qos0")

	reg := c.register(1, topic)
	if reg.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK return code %d, want accepted", reg.ReturnCode)
	}
	payload := allByteValues()
	c.publishQoS0(reg.TopicID, payload)

	m := obs.next()
	if m.Topic != topic || m.Retain || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("broker got topic %q retain %v payload % x; want topic %q, payload of %d bytes",
			m.Topic, m.Retain, m.Payload, topic, len(payload))
	}
}

// TestPublishQoS0UnknownTopicIDDropped checks that nothing reaches the broker for an unknown id. The
// observer subscribes to a wildcard that covers the sentinel topic, so the sentinel must be the first
// delivery. The REGACK that follows shows the gateway sent no PUBACK for the QoS 0 publishes.
func TestPublishQoS0UnknownTopicIDDropped(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	const sentinel = "e2e/unknown/sentinel"
	obs := newObserver(t, b.addr, "e2e-unknown-obs", "e2e/unknown/#")
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-unknown")

	reg := c.register(1, sentinel)
	if reg.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("REGACK return code %d, want accepted", reg.ReturnCode)
	}
	const neverIssued = 0xFFFE
	if reg.TopicID == neverIssued {
		t.Fatalf("test assumption broken: topic id %#x was issued", neverIssued)
	}
	c.publishQoS0(neverIssued, []byte("must be dropped"))
	c.publishQoS0(reg.TopicID, []byte("sentinel"))

	m := obs.next()
	if m.Topic != sentinel || string(m.Payload) != "sentinel" {
		t.Fatalf("first delivery on e2e/unknown/# was topic %q payload %q; want the sentinel only", m.Topic, m.Payload)
	}

	// Any PUBACK for the dropped publish would arrive before this REGACK and fail the expect.
	after := c.register(2, "e2e/unknown/after")
	if after.ReturnCode != mqttsn.ReturnAccepted || after.MsgID != 2 {
		t.Fatalf("REGACK after dropped publish: %+v", after)
	}
}

// allByteValues returns 256 bytes covering every value, so binary fidelity is checked, not only ASCII.
func allByteValues() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}
