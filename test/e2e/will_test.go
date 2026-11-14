package e2e

import (
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// willTimeoutWait covers the gateway's 10 s will handshake timeout plus the time to read the DISCONNECT.
const willTimeoutWait = 15 * time.Second

// connectWithWill runs the will handshake for CONNECT with Will=1: it expects WILLTOPICREQ, sends the will
// topic, expects WILLMSGREQ, sends the will message and returns the CONNACK. The topic must not be empty.
func connectWithWill(t *testing.T, c *snClient, clientID string, will mqttsn.WillTopic, payload []byte) mqttsn.Connack {
	t.Helper()
	c.send(mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: clientID})
	expect[mqttsn.WillTopicReq](t, c, waitShort)
	c.send(will)
	expect[mqttsn.WillMsgReq](t, c, waitShort)
	c.send(mqttsn.WillMsg{Payload: payload})
	return expect[mqttsn.Connack](t, c, waitLong)
}

// TestWillHandshakeQoS1Retain: the full will handshake with QoS 1 and retain gets CONNACK Accepted.
func TestWillHandshakeQoS1Retain(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	ack := connectWithWill(t, c, "e2e-will-qos1", mqttsn.WillTopic{QoS: 1, Retain: true, Topic: "e2e/will/qos1-retain"}, []byte("offline"))
	if ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK return code %d, want accepted", ack.ReturnCode)
	}
}

// TestWillEmptyTopicConnectsWithoutWill: an empty WILLTOPIC gets CONNACK Accepted with no WILLMSGREQ before it.
func TestWillEmptyTopicConnectsWithoutWill(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "e2e-will-empty"})
	expect[mqttsn.WillTopicReq](t, c, waitShort)
	c.send(mqttsn.WillTopic{})
	// A WILLMSGREQ would arrive here instead of CONNACK and fail the expect.
	ack := expect[mqttsn.Connack](t, c, waitLong)
	if ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("CONNACK return code %d, want accepted", ack.ReturnCode)
	}
}

// TestWillQoS2RefusedThenConnectAccepted: a QoS 2 will gets CONNACK Not supported and no session, so a valid
// CONNECT on the same socket is accepted.
func TestWillQoS2RefusedThenConnectAccepted(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)

	c.send(mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "e2e-will-qos2"})
	expect[mqttsn.WillTopicReq](t, c, waitShort)
	c.send(mqttsn.WillTopic{QoS: 2, Topic: "e2e/will/qos2"})
	ack := expect[mqttsn.Connack](t, c, waitLong)
	if ack.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("CONNACK return code %d, want not supported", ack.ReturnCode)
	}

	connect(t, c, "e2e-after-will-qos2")
}

// TestWillHandshakeTimeoutFreesSlot: with max_clients 1, a client that never answers WILLTOPICREQ holds the only
// slot until the handshake times out. Then it gets DISCONNECT, and a second client can connect.
func TestWillHandshakeTimeoutFreesSlot(t *testing.T) {
	b := startBroker(t)
	gw := startGatewayMax(t, b.addr, 1)
	silent := newSNClient(t, gw.addr)
	waiting := newSNClient(t, gw.addr)

	silent.send(mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "e2e-will-silent"})
	expect[mqttsn.WillTopicReq](t, silent, waitShort)

	waiting.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "e2e-will-waiting"})
	if ack := expect[mqttsn.Connack](t, waiting, waitShort); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("CONNACK return code %d during handshake, want congestion", ack.ReturnCode)
	}

	expect[mqttsn.Disconnect](t, silent, willTimeoutWait)
	connect(t, waiting, "e2e-will-waiting")
}
