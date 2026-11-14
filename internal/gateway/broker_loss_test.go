package gateway

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// startScriptBroker accepts one connection, answers CONNECT with CONNACK accepted, then hands the
// connection to after. When after returns, the connection is closed. The listener is closed on cleanup.
func startScriptBroker(t *testing.T, after func(conn net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := mqtt.ReadPacket(conn); err != nil {
			return
		}
		if _, err := conn.Write([]byte{0x20, 0x02, 0, 0}); err != nil {
			return
		}
		after(conn)
	}()
	t.Cleanup(func() { _ = ln.Close(); <-done })
	return ln.Addr().String()
}

// expectClientDisconnect checks that the client gets one DISCONNECT and then nothing else.
func expectClientDisconnect(t *testing.T, client *net.UDPConn) {
	t.Helper()
	m, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("waiting for DISCONNECT: %v", err)
	}
	if _, ok := m.(mqttsn.Disconnect); !ok {
		t.Fatalf("got %+v, want DISCONNECT", m)
	}
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("second datagram after DISCONNECT: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
}

func TestBrokerCloseDisconnectsClient(t *testing.T) {
	addr := startScriptBroker(t, func(net.Conn) {}) // returning closes the broker side
	gwAddr, client := startKeepaliveGateway(t, addr)
	connectClient(t, client, gwAddr, 60)
	expectClientDisconnect(t, client)
}

func TestBrokerDisconnectPacketDisconnectsClient(t *testing.T) {
	addr := startScriptBroker(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte{0xE0, 0x00})
		_, _ = io.Copy(io.Discard, conn) // until the gateway closes its side
	})
	gwAddr, client := startKeepaliveGateway(t, addr)
	connectClient(t, client, gwAddr, 60)
	expectClientDisconnect(t, client)
}

// TestBrokerPingTimeoutDisconnectsClient uses duration 1 (ping every 0.5 s). The broker never answers, so
// the second PINGREQ finds the first still outstanding and the gateway gives up on the broker.
func TestBrokerPingTimeoutDisconnectsClient(t *testing.T) {
	addr := startScriptBroker(t, func(conn net.Conn) {
		_, _ = io.Copy(io.Discard, conn) // reads PINGREQ and never answers
	})
	gwAddr, client := startKeepaliveGateway(t, addr)
	connectClient(t, client, gwAddr, 1)
	expectClientDisconnect(t, client)
}

// TestBrokerResetDisconnectsClient resets the broker connection (RST) after CONNACK. The reader usually
// sees the reset before any write, so this exercises the read path; the write path shares the same teardown.
func TestBrokerResetDisconnectsClient(t *testing.T) {
	addr := startScriptBroker(t, func(conn net.Conn) {
		_ = conn.(*net.TCPConn).SetLinger(0)
	})
	gwAddr, client := startKeepaliveGateway(t, addr)
	connectClient(t, client, gwAddr, 60)
	expectClientDisconnect(t, client)
}
