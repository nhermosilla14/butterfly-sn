package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// closedBrokerClient returns a real broker client whose connection is already closed, so every write fails.
// The listener only has to complete the TCP handshake, which the kernel does without an Accept call.
func closedBrokerClient(t *testing.T) *mqtt.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	cl, err := mqtt.Dial(t.Context(), ln.Addr().String(), testWaitLong)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	return cl
}

// writeLossSession is a session with one registered normal topic, as after a REGISTER.
func writeLossSession(t *testing.T) (*session, uint16) {
	t.Helper()
	s := &session{gw: &Gateway{log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	id, ok := s.topics.register("gw/write/loss")
	if !ok {
		t.Fatal("topic table rejected the test topic")
	}
	return s, id
}

func TestPublishQoS0WriteFailureIsError(t *testing.T) {
	s, id := writeLossSession(t)
	broker := closedBrokerClient(t)
	m := mqttsn.Publish{QoS: 0, TopicIDType: mqttsn.TopicIDNormal, TopicID: id, Data: []byte("x")}
	if err := s.handlePublish(broker, m, s.gw.log); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("QoS 0 PUBLISH on closed broker connection: %v, want net.ErrClosed", err)
	}
}

func TestPublishQoS1WriteFailureIsError(t *testing.T) {
	s, id := writeLossSession(t)
	broker := closedBrokerClient(t)
	m := mqttsn.Publish{QoS: 1, TopicIDType: mqttsn.TopicIDNormal, TopicID: id, MsgID: 7, Data: []byte("x")}
	if err := s.handlePublish(broker, m, s.gw.log); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("QoS 1 PUBLISH on closed broker connection: %v, want net.ErrClosed", err)
	}
	if _, busy := s.pending[7]; busy {
		t.Fatal("failed QoS 1 PUBLISH left its msg id pending")
	}
}

func TestPingreqWriteFailureIsError(t *testing.T) {
	s, _ := writeLossSession(t)
	broker := closedBrokerClient(t)
	// Idle for longer than the interval, so keepaliveDue tries to PINGREQ at once.
	const interval = 10 * time.Millisecond
	s.lastWrite = time.Now().Add(-time.Hour)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	if err := s.keepaliveDue(broker, interval, timer); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("PINGREQ on closed broker connection: %v, want net.ErrClosed", err)
	}
	if s.pingOutstanding {
		t.Fatal("failed PINGREQ marked a ping as outstanding")
	}
}
