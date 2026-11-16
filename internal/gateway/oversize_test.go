package gateway

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// brokerPublishLong is brokerPublish for bodies over 127 bytes: it encodes the remaining length as a variable
// byte integer.
func brokerPublishLong(flags byte, name string, id uint16, payload []byte) []byte {
	body := append([]byte{byte(len(name) >> 8), byte(len(name))}, name...)
	if (flags>>1)&3 == 1 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, payload...)
	out := []byte{0x30 | flags}
	for n := len(body); ; {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// TestOversizeQoS1DownlinkDroppedAckedNotRetried: a QoS 1 PUBLISH whose datagram exceeds one UDP datagram is dropped
// and acknowledged to the broker at once. Nothing is retransmitted, the session survives past several retry
// intervals, and the next normal message is delivered.
func TestOversizeQoS1DownlinkDroppedAckedNotRetried(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const topic = "gw/over/qos1"
	r.subscribeExact(1, topic, 1)
	r.brokerSend(brokerPublishLong(0x02, topic, 0x0606, make([]byte, 65520)))
	r.expectBrokerPuback(0x0606)

	if m, err := readSN(r.client, testSilenceWindow); err == nil {
		t.Fatalf("datagram for oversize PUBLISH: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
	r.expectNoBrokerPacket()
	r.session()

	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("next")))
	expectPublish(t, r.client, "next")
}

// TestOversizeQoS0DownlinkDropped: a QoS 0 PUBLISH too large to encode is dropped without a broker packet, and the
// next normal message is delivered.
func TestOversizeQoS0DownlinkDropped(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const topic = "gw/over/qos0"
	r.subscribeExact(1, topic, 0)
	r.brokerSend(brokerPublishLong(0x00, topic, 0, make([]byte, 70000)))

	if m, err := readSN(r.client, testSilenceWindow); err == nil {
		t.Fatalf("datagram for oversize PUBLISH: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
	r.expectNoBrokerPacket()

	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("next")))
	expectPublish(t, r.client, "next")
}

// TestOversizeRegisterDropsHeldAndForgetsName: a topic name too long for a REGISTER datagram drops the held QoS 1
// PUBLISH, acknowledged to the broker, and nothing is sent to the client. A later normal topic still registers.
func TestOversizeRegisterDropsHeldAndForgetsName(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	long := strings.Repeat("t", 65510)
	r.brokerSend(brokerPublishLong(0x02, long, 0x0707, []byte("dropped")))
	r.expectBrokerPuback(0x0707)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	const topic = "gw/over/after"
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("next")))
	reg := readAs[mqttsn.Register](t, r.client)
	if reg.TopicName != topic {
		t.Fatalf("REGISTER %+v, want topic %q", reg, topic)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	expectPublish(t, r.client, "next")
}

// TestOversizeBufferedMessageDroppedOnFlushThenPingresp: a buffered QoS 1 PUBLISH too large to send is dropped on
// the flush after a wake. The message behind it is still delivered, and the flush ends with PINGRESP.
func TestOversizeBufferedMessageDroppedOnFlushThenPingresp(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const topic = "gw/over/flush"
	r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 10)
	r.brokerSend(brokerPublishLong(0x02, topic, 0x0909, make([]byte, 65520)))
	r.expectBrokerPuback(0x0909)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("after")))
	expectSilence(t, r.client)

	wakePing(r)
	expectPublish(t, r.client, "after")
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}
