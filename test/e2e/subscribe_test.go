package e2e

import (
	"testing"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

func TestSubscribeExactReturnsAcceptedAndTopicID(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-sub-exact"))

	ack := c.subscribe(1, uniq("e2e/subscribe/exact"), 1)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.QoS != 1 || ack.TopicID == 0 || ack.MsgID != 1 {
		t.Fatalf("SUBACK %+v, want accepted, QoS 1, nonzero topic id, msg id 1", ack)
	}
}

func TestSubscribeWildcardReturnsTopicIDZero(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-sub-wild"))

	ack := c.subscribe(2, uniq("e2e/subscribe/wild")+"/#", 0)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.TopicID != 0 || ack.MsgID != 2 {
		t.Fatalf("SUBACK %+v, want accepted, topic id 0, msg id 2", ack)
	}
}

func TestUnsubscribeReturnsUnsubackWithMsgID(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-unsub"))
	topic := uniq("e2e/unsubscribe")

	if ack := c.subscribe(1, topic, 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	if un := c.unsubscribe(3, topic); un.MsgID != 3 {
		t.Fatalf("UNSUBACK %+v, want msg id 3", un)
	}
}
