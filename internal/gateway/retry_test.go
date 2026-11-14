package gateway

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// retryTune returns a gateway tuning that shortens the retry interval and sets the retry limit.
func retryTune(interval time.Duration, limit int) func(*Gateway) {
	return func(g *Gateway) {
		g.retryInterval = interval
		g.retryLimit = limit
	}
}

// expectSameRegister checks that got repeats want unchanged: same topic name, topic id and msg id.
func expectSameRegister(t *testing.T, got, want mqttsn.Register) {
	t.Helper()
	if got.MsgID != want.MsgID || got.TopicID != want.TopicID || got.TopicName != want.TopicName {
		t.Fatalf("REGISTER %+v, want a retransmission of %+v", got, want)
	}
}

// TestRetrySilentClientRegisterRepeatedThenDisconnect: a client that never answers gets the REGISTER repeated
// retry-limit times, then DISCONNECT. The session is gone and the will is published (broker closed without
// MQTT DISCONNECT).
func TestRetrySilentClientRegisterRepeatedThenDisconnect(t *testing.T) {
	r := startWillRigTuned(t, 10, retryTune(50*time.Millisecond, 2))
	r.openSession("retry-silent", 60, true)
	const topic = "gw/retry/silent"
	if _, err := r.broker.conn.Write(brokerPublish(0x00, topic, 0, []byte("held"))); err != nil {
		t.Fatal(err)
	}

	first := readAs[mqttsn.Register](t, r.client)
	if first.TopicName != topic {
		t.Fatalf("REGISTER %+v", first)
	}
	for i := 0; i < 2; i++ {
		expectSameRegister(t, readAs[mqttsn.Register](t, r.client), first)
	}
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoSessions(t, r.g)
	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
}

// TestRetryClientAnsweringSecondRegisterGetsHeldPublish: a client that answers the first retransmission gets the
// held PUBLISH, and no further REGISTER follows.
func TestRetryClientAnsweringSecondRegisterGetsHeldPublish(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 5))
	const topic = "gw/retry/answer"
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("held")))

	reg := readAs[mqttsn.Register](t, r.client)
	expectSameRegister(t, readAs[mqttsn.Register](t, r.client), reg)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	if m := expectPublish(t, r.client, "held"); m.TopicID != reg.TopicID {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, reg.TopicID)
	}
	expectSilence(t, r.client)
}

// TestRetryRegackDuringLastIntervalStopsSequence: a REGACK arriving during the last retry interval stops the
// sequence. Without it the client would be declared lost at the end of that interval.
func TestRetryRegackDuringLastIntervalStopsSequence(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(200*time.Millisecond, 1))
	const topic = "gw/retry/last"
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("held")))

	reg := readAs[mqttsn.Register](t, r.client)
	expectSameRegister(t, readAs[mqttsn.Register](t, r.client), reg) // the only retransmission
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	expectPublish(t, r.client, "held")
	expectSilence(t, r.client) // covers the end of the last interval, where a give-up would send DISCONNECT
	r.expectNoBrokerPacket()
}

// TestRetryRejectedRegackRemovesEntry: a REGACK with a rejection code also ends the retries. The held message is
// dropped and acknowledged to the broker once, with no retransmission and no give-up.
func TestRetryRejectedRegackRemovesEntry(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 1))
	const topic = "gw/retry/rejected"
	r.brokerSend(brokerPublish(0x02, topic, 0x0404, []byte("dropped")))

	reg := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnInvalidTopicID})
	r.expectBrokerPuback(0x0404)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}

// TestRetryQoS1PublishRepeatedWithDupUntilPuback: an unanswered QoS 1 PUBLISH is sent again with DUP set and the
// same msg id. The client's PUBACK stops the retries and the broker is acknowledged once.
func TestRetryQoS1PublishRepeatedWithDupUntilPuback(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 5))
	const topic = "gw/retry/qos1"
	id := r.subscribeExact(1, topic, 1)
	r.brokerSend(brokerPublish(0x02, topic, 0x0202, []byte("q1")))

	first := readAs[mqttsn.Publish](t, r.client)
	again := readAs[mqttsn.Publish](t, r.client)
	if first.DUP || !again.DUP || again.MsgID != 0x0202 || again.TopicID != id || string(again.Data) != "q1" || first.MsgID != again.MsgID {
		t.Fatalf("PUBLISH %+v then %+v, want DUP only on the second, same msg id %#x", first, again, 0x0202)
	}

	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: id, MsgID: 0x0202, ReturnCode: mqttsn.ReturnAccepted})
	r.expectBrokerPuback(0x0202)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()
}

// TestRetryQoS1GiveUpDisconnectsWithoutBrokerPuback: an unanswered QoS 1 PUBLISH is retried retry-limit times,
// then the client gets DISCONNECT. The broker gets no PUBACK, only the MQTT DISCONNECT of the teardown.
func TestRetryQoS1GiveUpDisconnectsWithoutBrokerPuback(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(50*time.Millisecond, 2))
	const topic = "gw/retry/giveup"
	r.subscribeExact(1, topic, 1)
	r.brokerSend(brokerPublish(0x02, topic, 0x0303, []byte("q1")))

	first := readAs[mqttsn.Publish](t, r.client)
	for i := 0; i < 2; i++ {
		again := readAs[mqttsn.Publish](t, r.client)
		if !again.DUP || again.MsgID != first.MsgID {
			t.Fatalf("retransmission %+v, want DUP and msg id %#x", again, first.MsgID)
		}
	}
	expectClientDisconnect(t, r.client)
	if p := r.nextBroker(); p.Type != pktDisconnect {
		t.Fatalf("broker packet type %d after give-up, want MQTT DISCONNECT only", p.Type)
	}
}

// TestRetryFlushMissingRegackEndsWithDisconnect: a wake-up flush whose REGISTER is never answered does not wait:
// the REGISTER is retried and then the client gets DISCONNECT, with no PINGRESP.
func TestRetryFlushMissingRegackEndsWithDisconnect(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(50*time.Millisecond, 2))
	const topic = "gw/retry/flush"
	sleepClient(t, r.client, r.gwAddr, 10)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("buffered")))
	expectSilence(t, r.client)

	wakePing(r)
	first := readAs[mqttsn.Register](t, r.client)
	for i := 0; i < 2; i++ {
		expectSameRegister(t, readAs[mqttsn.Register](t, r.client), first)
	}
	expectClientDisconnect(t, r.client)
	expectNoSessions(t, r.gw)
}

// TestRetrySleepingClientNotRetransmittedOrTimedOut: a REGISTER still unanswered when the client sleeps is dropped
// with the sleep. Nothing is sent while asleep, and the retry limit does not end the session. The next wake
// registers the held topic afresh and the REGACK completes it.
func TestRetrySleepingClientNotRetransmittedOrTimedOut(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(time.Second, 1))
	const topic = "gw/retry/sleep"
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("held")))
	readAs[mqttsn.Register](t, r.client)

	sleepClient(t, r.client, r.gwAddr, 10)
	if m, err := readSN(r.client, testSilenceWindow); err == nil {
		t.Fatalf("datagram while asleep: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
	r.session() // fails if the session ended: a sleeping client must not be timed out

	wakePing(r)
	again := readAs[mqttsn.Register](t, r.client)
	if again.TopicName != topic {
		t.Fatalf("REGISTER after wake %+v", again)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: again.TopicID, MsgID: again.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	if m := expectPublish(t, r.client, "held"); m.TopicID != again.TopicID {
		t.Fatalf("PUBLISH topic id %#x, want %#x", m.TopicID, again.TopicID)
	}
	readAs[mqttsn.Pingresp](t, r.client)
	expectSilence(t, r.client)
}
