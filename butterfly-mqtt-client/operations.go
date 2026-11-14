package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidTopic reports an empty topic name, or a topic name containing MQTT wildcards in Publish.
var ErrInvalidTopic = errors.New("mqtt: invalid topic")

// ErrSubscribeRefused reports a SUBACK that refused the subscription.
var ErrSubscribeRefused = errors.New("mqtt: subscription refused")

// Message is one PUBLISH received from the broker by ReadPublish.
type Message struct {
	Topic   string
	Payload []byte
	Retain  bool
}

// Publish sends a QoS 0 PUBLISH. The payload is sent unchanged, empty payloads included. QoS 0 has
// no packet identifier and no acknowledgement, so Publish returns once the bytes are written.
func (c *Client) Publish(topic string, payload []byte, retain bool) error {
	if topic == "" || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopic
	}
	body, err := appendString(nil, topic)
	if err != nil {
		return err
	}
	body = append(body, payload...)
	var flags byte
	if retain {
		flags = flagRetain
	}
	pkt, err := packet(typePublish, flags, body)
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// PublishQoS1 sends a QoS 1 PUBLISH with packet identifier id, which must not be 0. dup sets the DUP flag.
// It returns once the bytes are written; the broker's PUBACK arrives later and is read with ReadEvent.
// The caller chooses id and keeps track of which ids are outstanding.
func (c *Client) PublishQoS1(topic string, payload []byte, retain, dup bool, id uint16) error {
	if topic == "" || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopic
	}
	if id == 0 {
		return fmt.Errorf("%w: packet identifier 0", ErrMalformedPacket)
	}
	body, err := appendString(nil, topic)
	if err != nil {
		return err
	}
	body = append(body, byte(id>>8), byte(id))
	body = append(body, payload...)
	flags := byte(qos1Flag)
	if retain {
		flags |= flagRetain
	}
	if dup {
		flags |= flagDUP
	}
	pkt, err := packet(typePublish, flags, body)
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// Pingreq sends an MQTT PINGREQ. The broker answers with a PINGRESP, which is read with ReadEvent.
func (c *Client) Pingreq() error {
	pkt, err := packet(typePingreq, 0, nil)
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// EventKind tells which broker packet an Event holds.
type EventKind int

const (
	EventPuback     EventKind = iota + 1 // PUBACK; Event.PacketID is set
	EventPingresp                        // PINGRESP
	EventDisconnect                      // MQTT DISCONNECT: the broker closed the MQTT session
	EventSuback                          // SUBACK; PacketID, ReturnCode and GrantedQoS are set
	EventUnsuback                        // UNSUBACK; PacketID is set
	EventPublish                         // PUBLISH from the broker; Topic, Payload, QoS, Retain, DUP and PacketID are set
)

// SUBACK return codes. Event.ReturnCode carries one of them.
const (
	SubackQoS0  = 0x00
	SubackQoS1  = 0x01
	SubackError = 0x80
)

// Event is one broker packet returned by ReadEvent. Only the fields named for its Kind are meaningful.
// Event.PacketID is 0 for a QoS 0 PUBLISH. Payload is copied and is nil when empty.
type Event struct {
	Kind       EventKind
	PacketID   uint16
	ReturnCode byte   // SUBACK: SubackQoS0, SubackQoS1 or SubackError
	GrantedQoS byte   // SUBACK: granted QoS; 0 when refused
	Topic      string // PUBLISH
	Payload    []byte // PUBLISH
	QoS        byte   // PUBLISH: 0 or 1
	Retain     bool   // PUBLISH
	DUP        bool   // PUBLISH
}

// ReadEvent waits for the next broker packet that ReadEvent handles and returns it in arrival order,
// bounded by ctx. Packets are validated strictly: any other type, or a malformed PUBACK, PINGRESP,
// DISCONNECT, SUBACK, UNSUBACK or PUBLISH, is an error wrapping ErrMalformedPacket. PUBLISH supports
// QoS 0 and 1 only. It must not run concurrently with ReadPublish or Subscribe; see the package doc.
func (c *Client) ReadEvent(ctx context.Context) (Event, error) {
	p, err := c.readPacket(ctx)
	if err != nil {
		return Event{}, err
	}
	switch {
	case p.Type == typePuback && p.Flags == 0 && len(p.Body) == 2:
		return Event{Kind: EventPuback, PacketID: uint16(p.Body[0])<<8 | uint16(p.Body[1])}, nil
	case p.Type == typePingresp && p.Flags == 0 && len(p.Body) == 0:
		return Event{Kind: EventPingresp}, nil
	case p.Type == typeDisconnect && p.Flags == 0 && len(p.Body) == 0:
		return Event{Kind: EventDisconnect}, nil
	case p.Type == typeSuback && p.Flags == 0 && len(p.Body) == 3:
		return parseSuback(p.Body)
	case p.Type == typeUnsuback && p.Flags == 0 && len(p.Body) == 2:
		return Event{Kind: EventUnsuback, PacketID: uint16(p.Body[0])<<8 | uint16(p.Body[1])}, nil
	case p.Type == typePublish:
		return parsePublish(p.Flags, p.Body)
	}
	return Event{}, fmt.Errorf("%w: unexpected broker packet type %d flags %#x length %d", ErrMalformedPacket, p.Type, p.Flags, len(p.Body))
}

func parseSuback(body []byte) (Event, error) {
	code := body[2]
	switch code {
	case SubackQoS0, SubackQoS1:
		return Event{Kind: EventSuback, PacketID: packetID(body), ReturnCode: code, GrantedQoS: code}, nil
	case SubackError:
		return Event{Kind: EventSuback, PacketID: packetID(body), ReturnCode: code}, nil
	}
	return Event{}, fmt.Errorf("%w: SUBACK return code %#x", ErrMalformedPacket, code)
}

// parsePublish validates a broker PUBLISH: QoS 0 or 1, a non-empty topic name without wildcards and,
// for QoS 1, a nonzero packet identifier after the topic. The payload is everything after that.
func parsePublish(flags byte, body []byte) (Event, error) {
	qos := (flags >> 1) & 0x03
	dup := flags&flagDUP != 0
	if qos > 1 || (qos == 0 && dup) || len(body) < 2 {
		return Event{}, fmt.Errorf("%w: PUBLISH flags %#x or length %d", ErrMalformedPacket, flags, len(body))
	}
	n := int(body[0])<<8 | int(body[1])
	rest := body[2:]
	if n == 0 || len(rest) < n || strings.ContainsAny(string(rest[:n]), "+#") {
		return Event{}, fmt.Errorf("%w: PUBLISH topic", ErrMalformedPacket)
	}
	ev := Event{
		Kind:   EventPublish,
		Topic:  string(rest[:n]),
		QoS:    qos,
		Retain: flags&flagRetain != 0,
		DUP:    dup,
	}
	rest = rest[n:]
	if qos == 1 {
		if len(rest) < 2 || (rest[0] == 0 && rest[1] == 0) {
			return Event{}, fmt.Errorf("%w: PUBLISH packet identifier", ErrMalformedPacket)
		}
		ev.PacketID = packetID(rest)
		rest = rest[2:]
	}
	ev.Payload = append([]byte(nil), rest...)
	return ev, nil
}

func packetID(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

// SubscribeRequest sends SUBSCRIBE for filter at QoS qos (0 or 1) with packet identifier id, which must not
// be 0. It does not wait for SUBACK: the SUBACK arrives later and is read with ReadEvent.
func (c *Client) SubscribeRequest(filter string, qos byte, id uint16) error {
	if filter == "" {
		return ErrInvalidTopic
	}
	if qos > 1 {
		return fmt.Errorf("%w: requested QoS %d, want 0 or 1", ErrMalformedPacket, qos)
	}
	if id == 0 {
		return fmt.Errorf("%w: packet identifier 0", ErrMalformedPacket)
	}
	body := []byte{byte(id >> 8), byte(id)}
	body, err := appendString(body, filter)
	if err != nil {
		return err
	}
	body = append(body, qos)
	pkt, err := packet(typeSubscribe, flagSubscribe, body)
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// UnsubscribeRequest sends UNSUBSCRIBE for filter with packet identifier id, which must not be 0. It does
// not wait for UNSUBACK: the UNSUBACK arrives later and is read with ReadEvent.
func (c *Client) UnsubscribeRequest(filter string, id uint16) error {
	if filter == "" {
		return ErrInvalidTopic
	}
	if id == 0 {
		return fmt.Errorf("%w: packet identifier 0", ErrMalformedPacket)
	}
	body := []byte{byte(id >> 8), byte(id)}
	body, err := appendString(body, filter)
	if err != nil {
		return err
	}
	pkt, err := packet(typeUnsubscribe, flagSubscribe, body)
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// Puback acknowledges a QoS 1 PUBLISH from the broker with packet identifier id, which must not be 0.
func (c *Client) Puback(id uint16) error {
	if id == 0 {
		return fmt.Errorf("%w: packet identifier 0", ErrMalformedPacket)
	}
	pkt, err := packet(typePuback, 0, []byte{byte(id >> 8), byte(id)})
	if err != nil {
		return err
	}
	return c.write(pkt)
}

// Subscribe subscribes to filter at QoS 0 and waits for its SUBACK, bounded by ctx. It is meant for
// observers that read deliveries with ReadPublish. A connection uses either these observer reads
// (Subscribe, ReadPublish) or the event reader (ReadEvent), never both.
func (c *Client) Subscribe(ctx context.Context, filter string) error {
	if filter == "" {
		return ErrInvalidTopic
	}
	c.packetID++
	if c.packetID == 0 {
		c.packetID = 1
	}
	body := []byte{byte(c.packetID >> 8), byte(c.packetID)}
	body, err := appendString(body, filter)
	if err != nil {
		return err
	}
	body = append(body, 0) // requested QoS 0
	pkt, err := packet(typeSubscribe, flagSubscribe, body)
	if err != nil {
		return err
	}
	if err := c.write(pkt); err != nil {
		return ctxErr(ctx, err)
	}
	p, err := c.readPacket(ctx)
	if err != nil {
		return err
	}
	return checkSuback(p, c.packetID)
}

// ReadPublish waits for the next PUBLISH from the broker, bounded by ctx. Only QoS 0 is supported,
// so nothing is acknowledged. Any other packet type is an error. Use it only on observer connections; see Subscribe.
func (c *Client) ReadPublish(ctx context.Context) (Message, error) {
	p, err := c.readPacket(ctx)
	if err != nil {
		return Message{}, err
	}
	if p.Type != typePublish {
		return Message{}, fmt.Errorf("%w: expected PUBLISH, got packet type %d", ErrMalformedPacket, p.Type)
	}
	if qos := (p.Flags >> 1) & 0x03; qos != 0 {
		return Message{}, fmt.Errorf("mqtt: PUBLISH with QoS %d not supported", qos)
	}
	if len(p.Body) < 2 {
		return Message{}, fmt.Errorf("%w: PUBLISH too short", ErrMalformedPacket)
	}
	n := int(p.Body[0])<<8 | int(p.Body[1])
	if len(p.Body) < 2+n {
		return Message{}, fmt.Errorf("%w: PUBLISH topic longer than packet", ErrMalformedPacket)
	}
	return Message{
		Topic:   string(p.Body[2 : 2+n]),
		Payload: p.Body[2+n:],
		Retain:  p.Flags&flagRetain != 0,
	}, nil
}

// readPacket reads one packet, bounded by the deadline and cancellation of ctx.
func (c *Client) readPacket(ctx context.Context) (Packet, error) {
	deadline, _ := ctx.Deadline()
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return Packet{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.SetReadDeadline(time.Unix(1, 0)) })
	p, err := ReadPacket(c.conn)
	if !stop() {
		// ctx ended during the read, so its callback ran or is running; report the cancellation.
		return Packet{}, ctxErr(ctx, err)
	}
	if err != nil {
		return Packet{}, err
	}
	return p, nil
}

func checkSuback(p Packet, id uint16) error {
	if p.Type != typeSuback || len(p.Body) != 3 {
		return fmt.Errorf("%w: expected SUBACK", ErrMalformedPacket)
	}
	if got := uint16(p.Body[0])<<8 | uint16(p.Body[1]); got != id {
		return fmt.Errorf("%w: SUBACK packet id %d, want %d", ErrMalformedPacket, got, id)
	}
	if p.Body[2] == 0x80 {
		return ErrSubscribeRefused
	}
	return nil
}
