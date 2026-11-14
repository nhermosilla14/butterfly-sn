package e2e

import (
	"bytes"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestIdleClientSurvivesBrokerKeepalive connects with a 1 s keepalive, stays silent for longer than
// the broker's 1.5 s limit, then publishes QoS 1. Without gateway PINGREQs the broker would have dropped
// the session by then.
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
	time.Sleep(3 * time.Second)

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
