package gateway

import (
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// TestResleepShorterDurationExpiresOnNewSchedule: a re-sleep replaces the duration. A client that first sleeps for
// 60 s and re-sleeps for 1 s is expired on the 1 s schedule: the broker connection closes bare, so the will is
// published, and the session is removed.
func TestResleepShorterDurationExpiresOnNewSchedule(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("resleep-timer", 60, true)
	sleepClient(t, r.client, r.gwAddr, 60)
	sleepClient(t, r.client, r.gwAddr, 1)

	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoSessions(t, r.g)
}

// TestResleepWhileAsleepKeepsBufferedForNextPingreq: messages buffered before a re-sleep are kept, and messages
// that arrive after it are buffered too. The next PINGREQ delivers both, in order, then PINGRESP.
func TestResleepWhileAsleepKeepsBufferedForNextPingreq(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const topic = "gw/resleep/asleep"
	r.subscribeExact(1, topic, 0)
	sleepClient(t, r.client, r.gwAddr, 60)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("a")))
	expectSilence(t, r.client)

	sleepClient(t, r.client, r.gwAddr, 60)
	r.brokerSend(brokerPublish(0x00, topic, 0, []byte("b")))
	expectSilence(t, r.client)

	wakePing(r)
	expectPublish(t, r.client, "a")
	expectPublish(t, r.client, "b")
	readAs[mqttsn.Pingresp](t, r.client)
}

// TestResleepMidFlushKeepsOrderWithoutSecondPuback: a re-sleep during a wake-up flush that waits on an unanswered
// REGISTER returns the held message to the buffer in order, with no second broker PUBACK, and no PINGRESP for the
// interrupted flush. The topic is registered again on the next wake, which delivers both messages in order.
func TestResleepMidFlushKeepsOrderWithoutSecondPuback(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const known = "gw/resleep/known"
	r.subscribeExact(1, known, 0)
	sleepClient(t, r.client, r.gwAddr, 60)
	const unknown = "gw/resleep/unknown"
	r.brokerSend(brokerPublishLong(0x02, unknown, 0x0a0a, []byte("first")))
	r.expectBrokerPuback(0x0a0a)
	r.brokerSend(brokerPublish(0x00, known, 0, []byte("second")))
	expectSilence(t, r.client)

	wakePing(r)
	readAs[mqttsn.Register](t, r.client) // unanswered
	sleepClient(t, r.client, r.gwAddr, 60)
	expectSilence(t, r.client) // no retransmission, no PINGRESP
	r.expectNoBrokerPacket()

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	if reg.TopicName != unknown {
		t.Fatalf("REGISTER %+v, want the held topic", reg)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	first := expectPublish(t, r.client, "first")
	expectPublish(t, r.client, "second")
	readAs[mqttsn.Pingresp](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: first.TopicID, MsgID: first.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	r.expectNoBrokerPacket() // the PUBACK was absorbed: no second broker PUBACK
}

// TestResleepAbsorbsLatePubackOfFlushedQoS1: a QoS 1 message sent by a wake-up flush whose client PUBACK arrives
// after the client went back to sleep is absorbed: no second broker PUBACK, and the session is still usable.
func TestResleepAbsorbsLatePubackOfFlushedQoS1(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 5))
	const topic = "gw/resleep/late"
	r.subscribeExact(1, topic, 1)
	sleepClient(t, r.client, r.gwAddr, 60)
	r.brokerSend(brokerPublish(0x02, topic, 0x0c0c, []byte("q1")))
	r.expectBrokerPuback(0x0c0c)

	wakePing(r)
	m := expectPublish(t, r.client, "q1")
	readAs[mqttsn.Pingresp](t, r.client)
	sleepClient(t, r.client, r.gwAddr, 60)

	sendSN(t, r.client, r.gwAddr, mqttsn.Puback{TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	r.expectNoBrokerPacket()
	expectSilence(t, r.client)
	wakePing(r)
	readAs[mqttsn.Pingresp](t, r.client)
}

// TestResleepDurationZeroEndsSession: a DISCONNECT with duration 0 is not a re-sleep. It ends the session with MQTT
// DISCONNECT to the broker and DISCONNECT to the client.
func TestResleepDurationZeroEndsSession(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("resleep-zero", 60, true)
	sleepClient(t, r.client, r.gwAddr, 60)

	sendSN(t, r.client, r.gwAddr, mqttsn.Disconnect{Duration: 0, HasDuration: true})
	readAs[mqttsn.Disconnect](t, r.client)
	if !r.broker.waitClosed(t) {
		t.Fatal("broker got no MQTT DISCONNECT for a duration-0 DISCONNECT")
	}
	expectNoSessions(t, r.g)
}

// TestResleepDuringConnectWakeFlushReturnsToSleep: a re-sleep during a flush started by CONNECT, where the client is
// active, ends the flush and the client is asleep again. The held message is kept with no second broker PUBACK, and
// the next PINGREQ delivers it, then PINGRESP.
func TestResleepDuringConnectWakeFlushReturnsToSleep(t *testing.T) {
	r := startSubRigTuned(t, 60, retryTune(100*time.Millisecond, 2))
	const unknown = "gw/resleep/connect"
	sleepClient(t, r.client, r.gwAddr, 60)
	r.brokerSend(brokerPublishLong(0x02, unknown, 0x0d0d, []byte("held")))
	r.expectBrokerPuback(0x0d0d)

	wakeConnect(r)
	readAs[mqttsn.Register](t, r.client) // unanswered
	sleepClient(t, r.client, r.gwAddr, 60)
	expectSilence(t, r.client)
	r.expectNoBrokerPacket()

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	expectPublish(t, r.client, "held")
	readAs[mqttsn.Pingresp](t, r.client)
	r.expectNoBrokerPacket()
}
