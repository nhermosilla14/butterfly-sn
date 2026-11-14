package e2e

import (
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestBrokerDropDisconnectsClient cuts the proxied broker connection. The client gets DISCONNECT, and a
// REGISTER afterwards gets DISCONNECT from the unknown-sender rule instead of REGACK.
func TestBrokerDropDisconnectsClient(t *testing.T) {
	b := startBroker(t)
	proxy := startProxy(t, b.addr)
	gw := startGateway(t, proxy.addr())
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-loss-drop")

	proxy.dropAll()
	expect[mqttsn.Disconnect](t, c, waitLong)

	c.send(mqttsn.Register{MsgID: 1, TopicName: "e2e/loss/drop"})
	expect[mqttsn.Disconnect](t, c, waitShort)
}

// TestBrokerDropFreesClientSlot uses max_clients 1. While the first client is connected a second CONNECT
// gets CONNACK Congestion. After the first session loses its broker connection the slot is free.
func TestBrokerDropFreesClientSlot(t *testing.T) {
	b := startBroker(t)
	proxy := startProxy(t, b.addr)
	gw := startGatewayMax(t, proxy.addr(), 1)
	first := newSNClient(t, gw.addr)
	connect(t, first, "e2e-loss-slot-a")

	second := newSNClient(t, gw.addr)
	second.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "e2e-loss-slot-b"})
	if ack := expect[mqttsn.Connack](t, second, waitShort); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("CONNACK return code %d while slot taken, want congestion", ack.ReturnCode)
	}

	proxy.dropAll()
	expect[mqttsn.Disconnect](t, first, waitLong)

	// Retry within a bounded window: the session removal happens before the client's DISCONNECT is sent.
	deadline := time.Now().Add(waitLong)
	for {
		second.send(mqttsn.Connect{CleanSession: true, Duration: 60, ClientID: "e2e-loss-slot-b"})
		m, err := second.recv(waitShort)
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
			t.Fatalf("slot still taken after broker loss: CONNACK return code %d", ack.ReturnCode)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestBrokerDisconnectPacketDisconnectsClient has the proxy write an MQTT DISCONNECT towards the gateway.
// The client gets the same DISCONNECT as for a dropped connection.
func TestBrokerDisconnectPacketDisconnectsClient(t *testing.T) {
	b := startBroker(t)
	proxy := startProxy(t, b.addr)
	gw := startGateway(t, proxy.addr())
	c := newSNClient(t, gw.addr)
	connect(t, c, "e2e-loss-packet")

	proxy.injectToGateway([]byte{0xE0, 0x00})
	expect[mqttsn.Disconnect](t, c, waitLong)
}
