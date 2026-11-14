package gateway

import (
	"bytes"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// waitBrokerPublish reads broker packets until a PUBLISH to topic arrives, and fails on timeout. Other packets,
// such as keepalive PINGREQs, are skipped.
func waitBrokerPublish(t *testing.T, wb *willBroker, topic string) {
	t.Helper()
	deadline := time.After(testWaitLong)
	for {
		select {
		case p := <-wb.packets:
			if p.Type == 3 && bytes.Contains(p.Body, []byte(topic)) {
				return
			}
		case <-deadline:
			t.Fatalf("broker got no PUBLISH to %q", topic)
		}
	}
}

// TestWillUpdateRejectedNotSupported: WILLTOPICUPD, the empty WILLTOPICUPD and WILLMSGUPD each get a response
// with return code Not supported, and the broker connection is not reopened.
func TestWillUpdateRejectedNotSupported(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-upd-reject", 60, true)

	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{QoS: 1, Retain: true, Topic: "upd/topic"})
	if resp := readAs[mqttsn.WillTopicResp](t, r.client); resp.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("WILLTOPICRESP %+v, want Not supported", resp)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{})
	if resp := readAs[mqttsn.WillTopicResp](t, r.client); resp.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("empty WILLTOPICRESP %+v, want Not supported", resp)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("upd")})
	if resp := readAs[mqttsn.WillMsgResp](t, r.client); resp.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("WILLMSGRESP %+v, want Not supported", resp)
	}
	if n := len(r.broker.accepted); n != 1 {
		t.Fatalf("broker accepted %d connections after updates, want the session's 1", n)
	}
}

// TestWillUpdateKeepsSessionAndWill: after the rejected updates the session still publishes and answers PINGREQ,
// the broker connection stays open, and losing the client publishes the will given at CONNECT.
func TestWillUpdateKeepsSessionAndWill(t *testing.T) {
	r := startWillRig(t, 10)
	connect := r.openSession("will-upd-keep", 1, true)

	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{Topic: "upd/other"})
	readAs[mqttsn.WillTopicResp](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("other")})
	readAs[mqttsn.WillMsgResp](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.Register{MsgID: 1, TopicName: "dev/after-update"})
	reg := readAs[mqttsn.Regack](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.Publish{QoS: 0, TopicID: reg.TopicID, Data: []byte("still here")})
	waitBrokerPublish(t, r.broker, "dev/after-update")
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client)

	if n := len(r.broker.accepted); n != 1 {
		t.Fatalf("broker accepted %d connections, want 1", n)
	}
	if !bytes.Contains(connect.Body, []byte("will/will-upd-keep")) || !bytes.Contains(connect.Body, []byte("gone")) {
		t.Fatalf("broker CONNECT lacks the original will: % x", connect.Body)
	}
	// The client goes silent: the broker must get the bare close that publishes the original will.
	if r.broker.waitClosed(t) {
		t.Fatal("broker got MQTT DISCONNECT, want a bare close so the will is published")
	}
	readAs[mqttsn.Disconnect](t, r.client)
}

// TestWillUpdateDuringWillHandshakeIgnored: a WILLTOPICUPD sent during the CONNECT will handshake gets no
// answer, and the handshake still completes with the will it was given.
func TestWillUpdateDuringWillHandshakeIgnored(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.Connect{CleanSession: true, Will: true, Duration: 60, ClientID: "will-upd-hs"})
	readAs[mqttsn.WillTopicReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{QoS: 1, Topic: "upd/hs"})
	expectSilence(t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopic{QoS: 1, Topic: "will/hs"})
	readAs[mqttsn.WillMsgReq](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("upd")})
	expectSilence(t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsg{Payload: []byte("gone")})

	connect := r.broker.nextConnect(t)
	r.broker.allow()
	readAs[mqttsn.Connack](t, r.client)
	if !bytes.Contains(connect.Body, []byte("will/hs")) || bytes.Contains(connect.Body, []byte("upd/hs")) {
		t.Fatalf("broker CONNECT will is not the handshake will: % x", connect.Body)
	}
}

// TestWillUpdateAsleepIgnored: a sleeping client gets no answer to either update.
func TestWillUpdateAsleepIgnored(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-upd-asleep", 60, true)
	sleepClient(t, r.client, r.gwAddr, 5)

	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{Topic: "upd/asleep"})
	expectSilence(t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("asleep")})
	expectSilence(t, r.client)
}

// TestWillUpdateAnsweredDuringWakeFlush: while a wake flush is waiting for a REGACK the client is awake, so both
// updates are answered with Not supported.
func TestWillUpdateAnsweredDuringWakeFlush(t *testing.T) {
	r := startSubRigDuration(t, 60)
	sleepClient(t, r.client, r.gwAddr, 5)
	r.brokerSend(brokerPublish(0x00, "gw/upd/flush", 0, []byte("held")))
	expectSilence(t, r.client)

	wakePing(r)
	reg := readAs[mqttsn.Register](t, r.client)

	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{Topic: "upd/flush"})
	if resp := readAs[mqttsn.WillTopicResp](t, r.client); resp.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("WILLTOPICRESP %+v, want Not supported", resp)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("upd")})
	if resp := readAs[mqttsn.WillMsgResp](t, r.client); resp.ReturnCode != mqttsn.ReturnNotSupported {
		t.Fatalf("WILLMSGRESP %+v, want Not supported", resp)
	}

	sendSN(t, r.client, r.gwAddr, mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})
	expectPublish(t, r.client, "held")
	readAs[mqttsn.Pingresp](t, r.client)
}

// TestWillUpdateFromUnknownSenderDisconnects: an update from an address with no session gets DISCONNECT, and
// the broker is not contacted.
func TestWillUpdateFromUnknownSenderDisconnects(t *testing.T) {
	r := startWillRig(t, 10)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillTopicUpd{Topic: "upd/stranger"})
	readAs[mqttsn.Disconnect](t, r.client)
	sendSN(t, r.client, r.gwAddr, mqttsn.WillMsgUpd{Payload: []byte("stranger")})
	readAs[mqttsn.Disconnect](t, r.client)
	expectNoBrokerContact(t, r.broker)
}

// TestWillUpdateMalformedIgnored: malformed updates get no answer and do not end the session, which still
// answers PINGREQ afterwards.
func TestWillUpdateMalformedIgnored(t *testing.T) {
	r := startWillRig(t, 10)
	r.openSession("will-upd-bad", 60, true)

	// WILLTOPICUPD with flags but no topic, and one whose length header disagrees with its size.
	for _, raw := range [][]byte{{3, 0x1A, 0x20}, {4, 0x1A, 0x00}} {
		if _, err := r.client.WriteToUDP(raw, r.gwAddr); err != nil {
			t.Fatal(err)
		}
		expectSilence(t, r.client)
	}
	sendSN(t, r.client, r.gwAddr, mqttsn.Pingreq{})
	readAs[mqttsn.Pingresp](t, r.client)
}
