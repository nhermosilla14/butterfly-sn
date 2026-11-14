package gateway

import (
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// The broker publishes a will only when its connection ends without MQTT DISCONNECT. These tests check which
// ending the gateway picks for each way a session can end.

// TestClientTimeoutWithWillClosesBrokerWithoutDisconnect: a will-bearing client goes silent. The broker
// connection closes without MQTT DISCONNECT, and the client still gets DISCONNECT.
func TestClientTimeoutWithWillClosesBrokerWithoutDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-timeout", 1, true)

	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}

// TestClientTimeoutWithoutWillSendsDisconnect: a will-less client goes silent. The broker gets MQTT DISCONNECT.
func TestClientTimeoutWithoutWillSendsDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("no-will-timeout", 1, false)

	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT for a will-less session")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}

// TestShutdownWithWillClosesBrokerWithoutDisconnect: gateway shutdown closes a will-bearing session's broker
// connection without MQTT DISCONNECT. The client gets DISCONNECT and Run returns nil.
func TestShutdownWithWillClosesBrokerWithoutDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-shutdown", 60, true)

	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}

// TestShutdownWithoutWillSendsDisconnect: gateway shutdown sends MQTT DISCONNECT for a will-less session.
func TestShutdownWithoutWillSendsDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("no-will-shutdown", 60, false)

	if err := r.stop(); err != nil {
		t.Fatalf("gateway Run: %v", err)
	}
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT for a will-less session")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}

// TestExplicitDisconnectWithWillSendsDisconnect: a client DISCONNECT is graceful even when a will is stored.
func TestExplicitDisconnectWithWillSendsDisconnect(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-quit", 60, true)

	sendSN(t, r.client, r.gwAddr, mqttsn.Disconnect{})
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT after client DISCONNECT")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}
