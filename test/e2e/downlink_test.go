package e2e

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	mqtt "github.com/nhermosilla14/butterfly-mqtt-client"
	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

// publisher is a plain MQTT client connected straight to the broker. It is the source of downlink messages.
type publisher struct {
	t  *testing.T
	cl *mqtt.Client
}

// newPublisher connects clientID to the broker.
func newPublisher(t *testing.T, brokerAddr, clientID string) *publisher {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitShort)
	defer cancel()
	cl, err := mqtt.Dial(ctx, brokerAddr, waitShort)
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	if err := cl.Connect(ctx, mqtt.ConnectOptions{ClientID: clientID, CleanSession: true, KeepAlive: 60}); err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	return &publisher{t: t, cl: cl}
}

// publish sends a QoS 0 PUBLISH.
func (p *publisher) publish(topic string, payload []byte) {
	p.t.Helper()
	if err := p.cl.Publish(topic, payload, false); err != nil {
		p.t.Fatalf("publisher PUBLISH: %v", err)
	}
}

// publishQoS1 sends a QoS 1 PUBLISH with packet id id. The broker's PUBACK is not read.
func (p *publisher) publishQoS1(topic string, payload []byte, id uint16) {
	p.t.Helper()
	if err := p.cl.PublishQoS1(topic, payload, false, false, id); err != nil {
		p.t.Fatalf("publisher QoS 1 PUBLISH: %v", err)
	}
}

// expectNoDatagram fails if the gateway sends anything to c within the bounded window.
func expectNoDatagram(t *testing.T, c *snClient, window time.Duration) {
	t.Helper()
	if m, err := c.recv(window); err == nil {
		t.Fatalf("unexpected datagram: %+v", m)
	} else if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waiting for silence: %v", err)
	}
}

// downlinkWindow bounds how long a test waits to confirm that the gateway sent nothing.
const downlinkWindow = 500 * time.Millisecond

func TestDownlinkSubscribedTopicDeliveredWithoutRegister(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/downlink/exact")
	pub := newPublisher(t, b.addr, uniq("e2e-down-exact-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-down-exact"))

	ack := c.subscribe(1, topic, 0)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.TopicID == 0 {
		t.Fatalf("SUBACK %+v, want accepted with a topic id", ack)
	}
	payload := allByteValues()
	pub.publish(topic, payload)

	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicID != ack.TopicID || m.QoS != 0 || m.Retain || m.DUP || string(m.Data) != string(payload) {
		t.Fatalf("PUBLISH topic id %#x QoS %d payload of %d bytes, want topic id %#x, QoS 0, payload of %d bytes",
			m.TopicID, m.QoS, len(m.Data), ack.TopicID, len(payload))
	}
}

func TestDownlinkWildcardUnregisteredTopicRegistersFirst(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	base := uniq("e2e/downlink/wild")
	pub := newPublisher(t, b.addr, uniq("e2e-down-wild-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-down-wild"))

	if ack := c.subscribe(1, base+"/#", 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("SUBACK %+v, want accepted", ack)
	}
	topic := base + "/new"
	payload := []byte("announced first")
	pub.publish(topic, payload)

	reg := expect[mqttsn.Register](t, c, waitLong)
	if reg.TopicName != topic || reg.TopicID == 0 {
		t.Fatalf("REGISTER %+v, want topic %q with a nonzero topic id", reg, topic)
	}
	c.send(mqttsn.Regack{TopicID: reg.TopicID, MsgID: reg.MsgID, ReturnCode: mqttsn.ReturnAccepted})

	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.TopicID != reg.TopicID || string(m.Data) != string(payload) {
		t.Fatalf("PUBLISH %+v, want topic id %#x and payload %q", m, reg.TopicID, payload)
	}
}

func TestDownlinkQoS1AcknowledgedByClientAndFlowContinues(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	topic := uniq("e2e/downlink/qos1")
	pub := newPublisher(t, b.addr, uniq("e2e-down-qos1-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-down-qos1"))

	ack := c.subscribe(1, topic, 1)
	if ack.ReturnCode != mqttsn.ReturnAccepted || ack.QoS != 1 {
		t.Fatalf("SUBACK %+v, want accepted at QoS 1", ack)
	}
	pub.publishQoS1(topic, []byte("qos1 downlink"), 7)

	m := expect[mqttsn.Publish](t, c, waitLong)
	if m.QoS != 1 || m.MsgID == 0 || m.TopicID != ack.TopicID || string(m.Data) != "qos1 downlink" {
		t.Fatalf("PUBLISH %+v, want QoS 1 with a nonzero msg id and topic id %#x", m, ack.TopicID)
	}
	c.send(mqttsn.Puback{TopicID: m.TopicID, MsgID: m.MsgID, ReturnCode: mqttsn.ReturnAccepted})

	pub.publish(topic, []byte("after ack"))
	later := expect[mqttsn.Publish](t, c, waitLong)
	if later.QoS != 0 || later.TopicID != ack.TopicID || string(later.Data) != "after ack" {
		t.Fatalf("PUBLISH after client PUBACK %+v, want QoS 0 payload %q", later, "after ack")
	}
}

func TestDownlinkFullTableDropsNewTopicWithoutRegister(t *testing.T) {
	b := startBroker(t)
	gw := startGateway(t, b.addr)
	base := uniq("e2e/downlink/full")
	pub := newPublisher(t, b.addr, uniq("e2e-down-full-pub"))
	c := newSNClient(t, gw.addr)
	connect(t, c, uniq("e2e-down-full"))

	for i := 1; i <= 50; i++ {
		if reg := c.register(uint16(i), base+"/"+strconv.Itoa(i)); reg.ReturnCode != mqttsn.ReturnAccepted {
			t.Fatalf("REGISTER %d: REGACK %+v, want accepted", i, reg)
		}
	}
	c.send(mqttsn.Subscribe{QoS: 0, TopicIDType: mqttsn.TopicIDNormal, MsgID: 51, TopicName: base + "/exact"})
	if ack := expect[mqttsn.Suback](t, c, waitLong); ack.ReturnCode != mqttsn.ReturnCongestion {
		t.Fatalf("exact SUBSCRIBE at full table: SUBACK %+v, want congestion", ack)
	}
	if ack := c.subscribe(52, base+"/#", 0); ack.ReturnCode != mqttsn.ReturnAccepted {
		t.Fatalf("wildcard SUBACK %+v, want accepted", ack)
	}
	pub.publish(base+"/unregistered", []byte("no room"))
	expectNoDatagram(t, c, downlinkWindow)
}
