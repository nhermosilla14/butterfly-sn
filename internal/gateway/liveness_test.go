package gateway

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// testSilenceWindow is a bounded idle period longer than the duration-1 inactivity limit (1.5 s).
const testSilenceWindow = 2500 * time.Millisecond

// expectPingresp sends PINGREQ and checks the next datagram is PINGRESP.
func expectPingresp(t *testing.T, client *net.UDPConn, gwAddr *net.UDPAddr, clientID string) {
	t.Helper()
	sendSN(t, client, gwAddr, mqttsn.Pingreq{ClientID: clientID})
	m, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("waiting for PINGRESP: %v", err)
	}
	if _, ok := m.(mqttsn.Pingresp); !ok {
		t.Fatalf("got %+v, want PINGRESP", m)
	}
}

func TestPingreqGetsPingrespAndSessionSurvives(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 60)

	expectPingresp(t, client, gwAddr, "")
	expectPingresp(t, client, gwAddr, "ignored-client-id")
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("PINGREQ was forwarded or answered twice: %+v", m)
	}
	select {
	case <-pb.pingreqs:
		t.Fatal("client PINGREQ was forwarded to the broker")
	default:
	}
}

func TestSilentClientIsDisconnectedAfterInactivityLimit(t *testing.T) {
	// The ping broker answers the broker keepalive, so only the client's silence can end the session.
	pb := startPingBroker(t)
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 1)
	start := time.Now()

	m, err := readSN(client, testWaitLong)
	if err != nil {
		t.Fatalf("waiting for DISCONNECT: %v", err)
	}
	if _, ok := m.(mqttsn.Disconnect); !ok {
		t.Fatalf("got %+v, want DISCONNECT", m)
	}
	if elapsed := time.Since(start); elapsed < 1200*time.Millisecond {
		t.Fatalf("client disconnected after %v, before its 1.5 s limit", elapsed)
	}
	select {
	case <-pb.done: // the broker saw the connection close
	case <-time.After(testWaitLong):
		t.Fatal("broker connection not closed after client timeout")
	}
	if m, err := readSN(client, testNoDatagram); err == nil {
		t.Fatalf("second datagram after DISCONNECT: %+v", m)
	}
}

func TestPeriodicPingreqKeepsClientAlive(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 1)

	// Six pings over 3 s: twice the inactivity limit. Each ping must be answered before the next is sent.
	for i := 0; i < 6; i++ {
		time.Sleep(500 * time.Millisecond)
		expectPingresp(t, client, gwAddr, "")
	}
}

func TestZeroDurationNeverTimesOut(t *testing.T) {
	pb := startPingBroker(t)
	t.Cleanup(func() { _ = pb.ln.Close(); <-pb.done })
	gwAddr, client := startKeepaliveGateway(t, pb.ln.Addr().String())
	connectClient(t, client, gwAddr, 0)

	if m, err := readSN(client, testSilenceWindow); err == nil {
		t.Fatalf("datagram for silent duration-0 client: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
	expectPingresp(t, client, gwAddr, "")
}
