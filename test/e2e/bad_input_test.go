package e2e

import (
	"bytes"
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestUnknownSenderGetsDisconnect: a valid non-CONNECT from an address with no session gets DISCONNECT.
func TestUnknownSenderGetsDisconnect(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/unknown/sender"})
	expect[mqttsn.Disconnect](t, c, waitLong)
}

// TestDisconnectedSenderGetsDisconnect: after DISCONNECT the session is gone, so later packets get DISCONNECT.
func TestDisconnectedSenderGetsDisconnect(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-disconnected-sender")

	c.send(mqttsn.Disconnect{})
	expect[mqttsn.Disconnect](t, c, waitLong)

	c.send(mqttsn.Register{MsgID: 2, TopicName: "e2e/disconnected/sender"})
	expect[mqttsn.Disconnect](t, c, waitLong)
}

// TestEmptyClientIDRejected: CONNECT with an empty client id gets DISCONNECT and creates no session, so a
// valid CONNECT on the same socket is accepted.
func TestEmptyClientIDRejected(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: ""})
	expect[mqttsn.Disconnect](t, c, waitLong)

	connect(t, c, "e2e-after-empty-id")
}

// TestClientIDPrefixCreatesIndependentSession: ids that differ only by a prefix are both accepted, and each
// session publishes through its own broker connection.
func TestClientIDPrefixCreatesIndependentSession(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	obs := newObserver(t, b.addr, "e2e-prefix-obs", "e2e/prefix/#")
	short := newSNClient(t, gw.addr)
	connect(t, short, "e2e-prefix")
	long := newSNClient(t, gw.addr)
	connect(t, long, "e2e-prefix-longer")

	const shortTopic, longTopic = "e2e/prefix/short", "e2e/prefix/long"
	shortReg := short.register(1, shortTopic)
	longReg := long.register(1, longTopic)
	short.publishQoS0(shortReg.TopicID, []byte("from short"))
	long.publishQoS0(longReg.TopicID, []byte("from long"))

	got := map[string]string{}
	for range 2 {
		m := obs.next()
		got[m.Topic] = string(m.Payload)
	}
	if got[shortTopic] != "from short" || got[longTopic] != "from long" {
		t.Fatalf("broker deliveries %v, want one from each session", got)
	}
}

// TestMalformedDatagramsIgnored: zero-length, truncated and oversized invalid datagrams get no reply, and a
// valid CONNECT on the same socket still succeeds. Any reply would arrive before the CONNACK, so expect fails on it.
func TestMalformedDatagramsIgnored(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	// Extended header declaring 2053 bytes inside a 2048-byte datagram: invalid, and larger than 1024.
	oversized := append([]byte{0x01, 0x08, 0x00, 0x05}, bytes.Repeat([]byte{0xAA}, 2044)...)
	malformed := map[string][]byte{
		"zero length":            {},
		"one byte":               {0x02},
		"truncated connect":      {0x07, 0x04, 0x04, 0x01, 0x00, 0x3C},
		"extended length short":  {0x01, 0x08, 0x01, 0x18},
		"oversized connack body": oversized,
	}
	for name, raw := range malformed {
		if _, err := c.conn.Write(raw); err != nil {
			t.Fatalf("%s: write: %v", name, err)
		}
	}
	connect(t, c, "e2e-malformed")
}
